from django.core.management.base import BaseCommand
from django.conf import settings

from datetime import datetime, timedelta
import pytz

from sqs.components.gisquery.models import Task

import logging
logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Clears stale running tasks to prevent Tasks table from deadlocking'

    def handle(self, *args, **options):

        task_ids = []
        errors = []
        earliest_date = (datetime.now() - timedelta(days=60)).replace(tzinfo=pytz.utc)
        logger.info('Running command {}'.format(__name__))

        try:
            # update status of all long running tasks (there should only be max. one at a time)
            task_qs = Task.objects.filter(status=Task.STATUS_RUNNING)
            for task in task_qs:
                if task.is_long_running:
                    task.status = Task.STATUS_MAX_RUNNING_TIME
                    task.save()
                    logger.info(f'Long running task removed from pending TaskQueue {task.id}')

        except Exception as e:
            err_msg = f'Error updating long running task {e}'
            logger.error('{}\n{}'.format(err_msg))
            errors.append(err_msg)

        cmd_name = __name__.split('.')[-1].replace('_', ' ').upper()
        err_str = '<strong style="color: red;">Errors: {}</strong>'.format(len(errors)) if len(errors)>0 else '<strong style="color: green;">Errors: 0</strong>'
        msg = '<p>{} completed. {}. Deleted IDs: {}</p>'.format(cmd_name, err_str, task_ids)
        logger.info(msg)
        print(msg) # will redirect to cron_tasks.log file, by the parent script

