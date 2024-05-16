#from django.db import models
from django.contrib.gis.db import models
#from django.contrib.postgres.fields.jsonb import JSONField
from django.db.models import JSONField
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.contrib.postgres.aggregates.general import ArrayAgg
from django.db.models import Count
from django.utils import timezone

from reversion import revisions
from reversion.models import Version
import geopandas as gpd
import json
from pathlib import Path

from datetime import datetime, timedelta

from sqs.utils import HelperUtils, DATETIME_FMT
from sqs.decorators import traceback_exception_handler


# Next lin needed, to migrate ledger_api_clinet module
#from ledger_api_client.ledger_models import EmailUserRO as EmailUser

import logging
logger = logging.getLogger(__name__)


def earliest_date():
    ''' Return datetime <settings.STALE_TASKS_DAYS> ago '''
    return (datetime.now() - timedelta(days=settings.STALE_TASKS_DAYS)).replace(tzinfo=timezone.utc)

class RevisionedMixin(models.Model):
    """
    A model tracked by reversion through the save method.
    """
    def save(self, **kwargs):
        if kwargs.pop('no_revision', False):
            super(RevisionedMixin, self).save(**kwargs)
        else:
            with revisions.create_revision():
#                revisions.set_user(kwargs.pop('version_user', EmailUser.objects.get(id=255)))
#                if 'version_user' in kwargs:
#                    revisions.set_user(kwargs.pop('version_user', None))
                if 'version_comment' in kwargs:
                    revisions.set_comment(kwargs.pop('version_comment', ''))
                super(RevisionedMixin, self).save(**kwargs)

    @property
    def created_date(self):
        return Version.objects.get_for_object(self).last().revision.date_created

    @property
    def modified_date(self):
        return Version.objects.get_for_object(self).first().revision.date_created

    def get_obj_revision_dates(self):
        return [v.revision.date_created for v in Version.objects.get_for_object(self).order_by('revision__date_created')]

    def get_obj_revision_by_date(self, created_date):
        '''
        Usage:
            created_date = datetime.datetime(2023, 3, 22, 20, 17, 7, 987834)
            rev_obj = obj.get_obj_revision_by_date(created_date)
        '''
        try:
            return Version.objects.get_for_object(self).get(revision__date_created=created_date)._object_version.object
        except ObjectDoesNotExist as e:
            raise Exception('Revision for date {created_date} does not exist\n{e}')
        except Exception as e:
            raise

    class Meta:
        abstract = True


class ActiveLayerManager(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(active=True)

def geojson_file_path(instance, filename):
    # file will be uploaded to <settings.DATA_STORE>/<filename>
    return f'{settings.DATA_STORE}/{filename}'

class Layer(RevisionedMixin):

    name = models.CharField(max_length=128, unique=True)
    url = models.URLField(max_length=1024)
    #geojson = JSONField('Layer GeoJSON')
    geojson_file= models.FileField(upload_to=geojson_file_path)
    #attributes = models.TextField('Layer Attributes')
    attr_values = JSONField('Layer Attribute Values')
    version = models.IntegerField(editable=False, default=0)
    active = models.BooleanField(default=True)

    objects = models.Manager()
    active_layers = ActiveLayerManager()
    
    def save(self, *args, **kwargs):
        self.version = self.version + 1
        super().save(*args, **kwargs)

    @property
    def attributes (self):
        return [attr_val['attribute'] for attr_val in self.attr_values]

    @property
    @traceback_exception_handler
    def to_gdf(self):
        ''' Layer to Geo Dataframe (converted to settings.CRS ['epsg:4326']) '''
        #gdf = gpd.read_file(json.dumps(self.geojson_file.path))

        if not Path(self.geojson_file.path).is_file():
            #logger.warn(f'File for layer {self.name} Not Found: {self.geojson_file.path}')
            #return None
            raise Exception(f'File for layer {self.name} Not Found: {self.geojson_file.path}')

        return gpd.read_file(self.geojson_file.path)

    def get_obj_version_ids(self):
        ''' lists all versions for current layer '''
        try:
            return [dict(version=v.field_dict['version'], created_date=v.revision.date_created) for v in Version.objects.get_for_object(self).order_by('revision__date_created')]
        except Exception as e:
            raise
        
    def get_obj_revision_by_version(self, version_id):
        ''' return specific layer obj for given version_id '''
        version_obj = None
        try:
            filtered_versions = [v for v in Version.objects.get_for_object(self) if v.field_dict['version'] == version_id]
            if len(filtered_versions) == 0:
                logger.info(f'Version ID Not Found: {version_id}')
            elif len(filtered_versions) > 1:
                logger.info(f'Multiple Version ID\'s Found: {filtered_versions}')
            else:
                version_obj = filtered_versions[0]._object_version.object 
        except IndexError as e:
            version_ids = [v.field_dict['version'] for v in Version.objects.get_for_object(self).order_by('revision__date_created')]
            logger.error(f'Available revision versions:\n{version_ids}')
        except Exception as e:
            raise

        return version_obj 
            
    class Meta:
        app_label = 'sqs'

    def __str__(self):
        return f'{self.name}, version {self.version}'

class LayerRequestLog(models.Model):
    FULL = 'FULL'
    PARTIAL = 'PARTIAL'
    SINGLE = 'SINGLE'
    REQUEST_TYPE_CHOICES = (
        (FULL, 'FULL'),
        (PARTIAL, 'PARTIAL'),
        (SINGLE, 'SINGLE'),
    )

    request_type = models.CharField(max_length=40, choices=REQUEST_TYPE_CHOICES, default=REQUEST_TYPE_CHOICES[0][0])
    system = models.CharField('Application name', max_length=64)
    app_id = models.SmallIntegerField('Application ID')
    data = JSONField('Request query from external system')
    response = JSONField('Response from SQS', default=dict)
    when = models.DateTimeField(auto_now_add=True, null=False, blank=False)

    @classmethod
    def create_log(self, data, request_type):
        system = data['system']
        app_id = data['proposal']['id']

        log = LayerRequestLog.objects.create(system=system, app_id=app_id, request_type=request_type, data=data)
        return log

    def request_details(self, system=None, app_id=None, request_type='FULL', show_layers=False):
        '''
        Get history of layers requested from external systems
        request_type: FULL | PARTIAL | SINGLE
        '''

        if system is None and app_id is None:
            system = self.system
            app_id = self.app_id

        if (system is None and app_id is not None) or (system is not None and app_id is None):
            logger.error(f'Must specify both <system> and <app_id>, or specify neither') 
            return {}

        res = {}
        request_log_qs = LayerRequestLog.objects.filter(system=system, app_id=app_id, request_type=request_type).order_by('-when')
        if request_log_qs.count() > 0:
            request_log = request_log_qs[0]
 
            masterlist_questions = request_log.data['masterlist_questions']

            layers_in_request = HelperUtils.get_layer_names(masterlist_questions)
            existing_layers = list(Layer.active_layers.filter(name__in=layers_in_request).values_list('name', flat=True))
            new_layers = list(set(existing_layers).symmetric_difference(set(layers_in_request)))

            res = dict(
                system=system,
                app_id=app_id,
                num_layers_in_request=len(layers_in_request),
                num_new_layers=len(new_layers),
            )

            if show_layers:
                res.update(
                    dict(
                        layers_in_request=layers_in_request,
                        new_layers=new_layers,
                    )
                )
        else:
           logger.warn(f'No LayerRequestLog instance found for {system}: app_id {app_id}, request_type: {request_type}') 

        return res

    def __str__(self):
        return f'{self.system}|{self.app_id}|{self.when.strftime(DATETIME_FMT)}'

    class Meta:
        app_label = 'sqs'
        ordering = ('-when',)


class ActiveQueueManager(models.Manager):
    ''' filter queued tasks and omit old (stale) queued tasks '''
    def get_queryset(self):
        earliest_date = (datetime.now() - timedelta(days=settings.STALE_TASKS_DAYS)).replace(tzinfo=timezone.utc)
        return super().get_queryset().filter(status=Task.STATUS_CREATED, created__gte=earliest_date)


class Task(RevisionedMixin):

    PRIORITY_HIGH = 1
    PRIORITY_NORMAL = 2
    PRIORITY_LOW = 3
    PRIORITY_CHOICES = (
	(PRIORITY_HIGH,   'High'),
	(PRIORITY_NORMAL, 'Normal'),
	(PRIORITY_LOW,    'Low'),
    )

    STATUS_FAILED = 'failed'
    STATUS_CREATED = 'created'
    STATUS_RUNNING = 'running'
    STATUS_COMPLETED = 'completed'
    STATUS_CANCELLED = 'cancelled'
    STATUS_ERROR = 'error'
    STATUS_MAX_QUEUE_TIME = 'max_queue_time'
    STATUS_MAX_RETRIES_REACHED = 'max_retries'
    STATUS_CHOICES = (
	(STATUS_FAILED,    'Failed'),
	(STATUS_CREATED,   'Created'),
	(STATUS_RUNNING,   'Running'),
	(STATUS_COMPLETED, 'Completed'),
	(STATUS_CANCELLED, 'Cancelled'),
	(STATUS_ERROR,     'Error'),
        (STATUS_MAX_QUEUE_TIME, 'Max_Queue_Time_Reached'),
        (STATUS_MAX_RETRIES_REACHED, 'Max_Retries_Reached'),
    )

    app_id      = models.PositiveIntegerField('Application ID')
    system      = models.CharField('Application name', max_length=100)
    requester   = models.CharField('Prefill Request User', max_length=100)
    script      = models.TextField('Script name')
    #data        = JSONField('Request query from external system')
    description = models.TextField('Task Description', null=True, blank=True)
    parameters  = models.TextField('Script Parameters', null=True, blank=True)
    status      = models.CharField('Task Status', choices=STATUS_CHOICES, default=STATUS_CREATED, max_length=32)
    priority    = models.PositiveSmallIntegerField('Task Priority', choices=PRIORITY_CHOICES, default=PRIORITY_NORMAL)
    start_time    = models.DateTimeField(null=True, blank=True)
    end_time    = models.DateTimeField(null=True, blank=True)
    stdout      = models.TextField(null=True, blank=True)
    stderr      = models.TextField(null=True, blank=True)
    request_log = models.OneToOneField(LayerRequestLog, on_delete=models.CASCADE, related_name='request_log', null=True, blank=True)
    created     = models.DateTimeField(default=timezone.now, editable=False) # jm: needed for ordering queue
    retries     = models.PositiveSmallIntegerField(default=0)

    objects = models.Manager()
    queued_jobs = ActiveQueueManager()

    class Meta:
        app_label = 'sqs'
        #ordering = ('created_date',)

    def __str__(self):
        return f'{self.id} {self.system}_{self.app_id}'

    @property
    def data(self):
        return self.request_log.data

    @property
    def queue(self):
        """ Returns the ordered task queue """
        #return Task.objects.filter(status=self.STATUS_CREATED).order_by('priority', 'created')
        return Task.queued_jobs.filter(status=self.STATUS_CREATED).order_by('priority', 'created')

    def next(self):
        """ Returns the next task in the queue """
        return self.queue.first()

    @property
    def position(self):
        """ Returns the position in the queue """
        return self.queue.filter(created__lte=self.created).count()

 
    def time_taken(self):
        """ Returns task duration in mins """
        if self.start_time and self.end_time:
            return round((self.end_time - self.start_time).total_seconds()/60., 2)
        return None


import reversion
#reversion.register(Layer, follow=['access_logs'])
reversion.register(Layer, follow=[])
reversion.register(LayerRequestLog)
reversion.register(Task)


