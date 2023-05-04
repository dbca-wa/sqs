#from django.db import models
from django.contrib.gis.db import models
#from django.contrib.postgres.fields.jsonb import JSONField
from django.db.models import JSONField
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.contrib.postgres.aggregates.general import ArrayAgg
from django.db.models import Count
from reversion import revisions
from reversion.models import Version
import geopandas as gpd
import json

from datetime import datetime
from pytz import timezone

from sqs.utils import HelperUtils, DATETIME_FMT


# Next lin needed, to migrate ledger_api_clinet module
#from ledger_api_client.ledger_models import EmailUserRO as EmailUser

import logging
logger = logging.getLogger(__name__)


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

class Layer(RevisionedMixin):

    name = models.CharField(max_length=128, unique=True)
    url = models.URLField(max_length=1024)
    geojson = JSONField('Layer GeoJSON')
    version = models.IntegerField(editable=False, default=0)
    active = models.BooleanField(default=True)

    objects = models.Manager()
    active_layers = ActiveLayerManager()
    
    def save(self, *args, **kwargs):
        self.version = self.version + 1
        super().save(*args, **kwargs)

    @property
    def to_gdf(self):
        ''' Layer to Geo Dataframe (converted to settings.CRS ['epsg:4326']) '''
        gdf = gpd.read_file(json.dumps(self.geojson))
        return gdf.to_crs(settings.CRS)

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
    system = models.CharField('Application name', max_length=64)
    app_id = models.SmallIntegerField('Application ID')
    data = JSONField('Request query from external system')
    response = JSONField('Response from SQS', default=[{}])
    when = models.DateTimeField(auto_now_add=True, null=False, blank=False)

    @classmethod
    def create_log(self, data):
        system = data['proposal']['system']
        app_id = data['proposal']['id']

        log = LayerRequestLog.objects.create(system=system, app_id=app_id, data=data)
        return log

    def request_details(self, system=None, app_id=None, show_layers=False):
        '''
        Get history of layers requested from external systems
        '''

        if system is None and app_id is None:
            system = self.system
            app_id = self.app_id

        if (system is None and app_id is not None) or (system is not None and app_id is None):
            logger.error(f'Must specify both <system> and <app_id>, or specify neither') 
            return {}

        res = {}
        request_log_qs = LayerRequestLog.objects.filter(system=system, app_id=app_id).order_by('-when')
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
           logger.warn(f'No LayerRequestLog instance found for {system}: app_id {app_id}') 

        return res

    def __str__(self):
        return f'{self.system}|{self.app_id}|{self.when.strftime(DATETIME_FMT)}'

    
    class Meta:
        app_label = 'sqs'
        ordering = ('-when',)


#class LayerRequestLog(models.Model):
#    layer_name = models.CharField(max_length=64)
#    system = models.CharField('Application name', max_length=64)
#    #when = models.DateTimeField(auto_now_add=True, null=False, blank=False)
#    when = models.DateTimeField()
#
#    @classmethod
#    def create_log(self, system, masterlist_questions):
#        now = datetime.now().astimezone(timezone(settings.TIME_ZONE))
#
#        bulk_list = []
#        layer_names = HelperUtils.get_layer_names(masterlist_questions)
#        for layer_name in layer_names:
#            bulk_list.append(
#                LayerRequestLog(layer_name=layer_name, system=system, when=now)
#            )
#        bulk_objs = LayerRequestLog.objects.bulk_create(bulk_list) 
#        return bulk_objs
#
#    def request_history(self, system, last=5, show_layers=False):
#        '''
#        Get history of layers requested from external systems
#        '''
#
#        if show_layers:
#            request_list = list(
#                LayerRequestLog.objects.filter(system='DAS').values('system','when').annotate(
#                    num_layers=Count('when'), 
#                    layer_list=ArrayAgg('layer_name'),
#                ).order_by('-when')[:last]
#            )
#
#            for record in request_list:
#                layers_in_request = record['layer_list']
#                cur_layers_in_sqs = list(Layer.active_layers.filter(name__in=layers_in_request).values_list('name', flat=True))
#                new_layers = list(set(cur_layers_in_sqs).symmetric_difference(set(layers_in_request)))
#                record.update(dict(new_layers=new_layers))
#        else:
#            request_list = list(
#                LayerRequestLog.objects.filter(system='DAS').values('system','when').annotate(
#                    num_layers=Count('when'), 
#                ).order_by('-when')[:last]
#            )
#
#        return request_list
#
#    def __str__(self):
#        return f'{self.layer_name}|{self.system}|{self.when.strftime(DATETIME_FMT)}'
#
#    
#    class Meta:
#        app_label = 'sqs'
#        ordering = ('-when',)

  
import reversion
#reversion.register(Layer, follow=['access_logs'])
reversion.register(Layer, follow=[])
reversion.register(LayerRequestLog)


