"""Remove obsolete timestamped layer data and its GeoJsonFile metadata.

For each selected layer, retain the newest ``--keep-versions`` directories
that have database records pointing to existing files, along with every
timestamped directory newer than ``--min-age-days``. Older eligible
directories are reported only by default. With ``--apply``, their GeoJsonFile
records are deleted before the directories themselves.

Cleanup is stopped if ``update_layers`` is running, and layers used by an
identifiable active prefill/refresh task are skipped. Layers without a valid
database-backed version are also skipped to avoid deleting data when the
current version cannot be established.
"""

from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import subprocess

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from sqs.components.gisquery.models import GeoJsonFile, Layer, Task
from sqs.utils import HelperUtils


TIMESTAMP_FORMAT = '%Y%m%dT%H%M%S'


class Command(BaseCommand):
    help = (
        'Remove obsolete timestamped layer directories and their GeoJsonFile metadata. '
        'Runs as a dry-run unless --apply is specified.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--keep-versions',
            type=int,
            default=3,
            help='Number of newest valid database-backed versions to retain per layer (default: 3).',
        )
        parser.add_argument(
            '--min-age-days',
            type=int,
            default=7,
            help='Retain all timestamp directories newer than this many days (default: 7).',
        )
        parser.add_argument(
            '--layer',
            action='append',
            dest='layers',
            help='Limit cleanup to a layer name. Specify more than once for multiple layers.',
        )
        parser.add_argument(
            '--apply',
            action='store_true',
            help='Delete eligible directories and obsolete GeoJsonFile records. Without this flag, report only.',
        )

    def handle(self, *args, **options):
        keep_versions = options['keep_versions']
        min_age_days = options['min_age_days']
        selected_layers = options['layers']
        apply_changes = options['apply']

        if keep_versions < 1:
            raise CommandError('--keep-versions must be at least 1.')
        if min_age_days < 0:
            raise CommandError('--min-age-days cannot be negative.')

        data_store = Path(settings.DATA_STORE).resolve()
        if not data_store.is_dir():
            raise CommandError(f'DATA_STORE does not exist or is not a directory: {data_store}')
        if self.update_layers_is_running():
            raise CommandError('Cleanup aborted: update_layers is currently running.')

        cutoff = timezone.now() - timedelta(days=min_age_days)
        queryset = Layer.objects.order_by('name')
        if selected_layers:
            queryset = queryset.filter(name__in=selected_layers)

        mode = 'APPLY' if apply_changes else 'DRY RUN'
        self.stdout.write(f'{mode}: retaining {keep_versions} valid versions and directories newer than {cutoff.isoformat()}.')

        totals = defaultdict(int)
        for layer in queryset:
            active_task_ids = self.get_active_task_ids_for_layer(layer.name)
            if active_task_ids:
                self.stdout.write(self.style.WARNING(
                    f'Skipping {layer.name}: used by active prefill/refresh task IDs {active_task_ids}.'
                ))
                continue

            cleanup_directories = self.get_cleanup_directories(layer, data_store, keep_versions, cutoff)
            if not cleanup_directories:
                continue

            for directory in cleanup_directories:
                file_records = GeoJsonFile.objects.filter(
                    layer=layer,
                    geojson_file__startswith=f'{directory.relative_to(data_store).as_posix()}/',
                )
                record_count = file_records.count()
                directory_size = self.directory_size(directory)
                self.stdout.write(
                    f'{mode}: {layer.name}/{directory.name} '
                    f'({directory_size / 1024 ** 2:.2f} MB, {record_count} GeoJsonFile records)'
                )

                if apply_changes:
                    if self.update_layers_is_running():
                        raise CommandError('Cleanup aborted: update_layers started while cleanup was running.')
                    active_task_ids = self.get_active_task_ids_for_layer(layer.name)
                    if active_task_ids:
                        self.stdout.write(self.style.WARNING(
                            f'Skipping {layer.name}: used by active prefill/refresh task IDs {active_task_ids}.'
                        ))
                        break
                    # Delete metadata first so no database record points at a removed directory.
                    with transaction.atomic():
                        file_records.delete()
                    shutil.rmtree(directory)

                totals['directories'] += 1
                totals['records'] += record_count
                totals['bytes'] += directory_size

        self.stdout.write(
            f'{mode} complete: {totals["directories"]} directories, '
            f'{totals["records"]} GeoJsonFile records, {totals["bytes"] / 1024 ** 2:.2f} MB.'
        )

    def get_cleanup_directories(self, layer, data_store, keep_versions, cutoff):
        layer_directory = data_store / layer.name
        if not layer_directory.is_dir():
            self.stdout.write(self.style.WARNING(f'Skipping {layer.name}: directory does not exist.'))
            return []

        timestamp_directories = [
            directory for directory in layer_directory.iterdir()
            if directory.is_dir() and self.timestamp(directory) is not None
        ]
        records_by_directory = defaultdict(list)
        for file_record in GeoJsonFile.objects.filter(layer=layer).order_by('-id'):
            record_directory = Path(file_record.geojson_file.path).parent
            if record_directory.parent == layer_directory and record_directory.is_dir():
                records_by_directory[record_directory].append(file_record)

        valid_directories = [
            directory for directory in records_by_directory
            if any(Path(record.geojson_file.path).is_file() for record in records_by_directory[directory])
        ]
        valid_directories.sort(
            key=lambda directory: max(record.id for record in records_by_directory[directory]),
            reverse=True,
        )
        retained_directories = set(valid_directories[:keep_versions])
        retained_directories.update(
            directory for directory in timestamp_directories if self.timestamp(directory) >= cutoff
        )

        if not valid_directories:
            self.stdout.write(self.style.WARNING(f'Skipping {layer.name}: no valid database-backed version exists.'))
            return []

        return sorted(
            (directory for directory in timestamp_directories if directory not in retained_directories),
            key=self.timestamp,
        )

    def get_active_task_ids_for_layer(self, layer_name):
        active_tasks = Task.objects.filter(
            status__in=[Task.STATUS_CREATED, Task.STATUS_RUNNING],
            request_log__isnull=False,
        ).select_related('request_log')
        task_ids = []
        for task in active_tasks:
            try:
                layers_in_task = HelperUtils.get_layer_names(task.data['masterlist_questions'])
            except (KeyError, TypeError):
                self.stdout.write(self.style.WARNING(
                    f'Unable to inspect layers for active task {task.id}; cleanup is skipped only when a layer can be identified.'
                ))
                continue
            if layer_name in layers_in_task:
                task_ids.append(task.id)
        return task_ids

    @staticmethod
    def update_layers_is_running():
        result = subprocess.run(
            ['pgrep', '-f', r'[p]ython.*manage.py update_layers'],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.returncode == 0

    @staticmethod
    def timestamp(directory):
        try:
            timestamp = datetime.strptime(directory.name, TIMESTAMP_FORMAT)
            return timezone.make_aware(timestamp, timezone.get_current_timezone())
        except ValueError:
            return None

    @staticmethod
    def directory_size(directory):
        return sum(file_path.stat().st_size for file_path in directory.rglob('*') if file_path.is_file())