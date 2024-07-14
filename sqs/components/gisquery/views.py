from django.conf import settings
from django.http import Http404, HttpResponse, HttpResponseRedirect, JsonResponse
from rest_framework import status
from http import HTTPStatus
from django.urls import reverse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views import View
from django.core.exceptions import ObjectDoesNotExist
from django.core.cache import cache
from django.utils import timezone

from datetime import datetime, timedelta
import time
import requests
import json
import unicodecsv
import pytz
import traceback

from sqs.components.gisquery.models import Layer, LayerRequestLog, Task
from sqs.utils.geoquery_utils import DisturbanceLayerQueryHelper, PointQueryHelper
from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData
from sqs.utils.loader_utils import DbLayerProvider

from sqs.components.api import models as api_models
from sqs.components.api import utils as api_utils
from sqs.decorators import ip_check_required, basic_exception_handler, traceback_exception_handler, apiview_response_exception_handler
from sqs.exceptions import LayerProviderException
from sqs.components.gisquery.utils import set_das_cache, clear_cache
from sqs.components.gisquery.utils.schema import is_valid_schema

import logging
logger = logging.getLogger(__name__)


class DisturbanceLayerView(View):
    queryset = Layer.objects.filter().order_by('id')

    @csrf_exempt
    def post(self, request):            
        """ Intersect user provided Shapefile/GeoJSON with SQS layers to infer required responses

        import requests
        from sqs.utils.das_tests.request_log.das_query import DAS_QUERY_JSON
        r = requests.post(url=f'http://localhost:8002/api/v1/das/spatial_query/', data={data: json.dumps(DAS_QUERY_JSON)})
        """
        def get_question_ids():
            try:
                ids = '_'.join([str(q['id']) for q in masterlist_questions[0]['questions']])
            except Exception as qe:
                ids = None
            return ids

        def normalise_datetime(when):
            ''' drop the decimal seconds, and set timezone UTC 
                when --> type is datetime.datetime
                         returns datetime.datetime
            '''
            return datetime.strptime(datetime.strftime(when, '%Y-%m-%dT%H:%M:%S'), '%Y-%m-%dT%H:%M:%S').replace(tzinfo=pytz.utc)


        start_time = time.time()
        cache_key = None
        try:
            data = json.loads(request.POST['data'])

            proposal = data.get('proposal')
            current_ts = proposal.get('current_ts') # only available following subsequent Prefill requests
            geojson = data.get('geojson')
            masterlist_questions = data.get('masterlist_questions')
            request_type = data.get('request_type')
            system = data.get('system')

            if proposal is None or proposal.get('schema') is None or proposal.get('id') is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No Proposal schema specified in Request'})
            if geojson is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No Shapefile/GeoJSON found for Proposal {proposal.get("id")}'})
            if len(masterlist_questions)==0:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No CDDP Masterlist Questions specified in Request'})
            if request_type is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No Request_Type specified in Request'})
            if system is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No System Name specified in Request'})

            # check if a previous request exists with a more recent timestamp
            # datetime.strptime('2023-07-04T10:53:17', '%Y-%m-%dT%H:%M:%S')
            qs_cur = LayerRequestLog.objects.filter(app_id=proposal['id'], request_type='FULL', system='DAS')
            if qs_cur.exists() and current_ts is not None:
                ts = datetime.strptime(current_ts, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=pytz.utc)
                last_query_date = normalise_datetime(qs_cur.latest('when').when)
                if last_query_date > ts:
                    logger.info(f'Previously executed query exists from {last_query_date}. Last proposal prefill time {ts}. Updating from cached results ...')
                    try: 
                        return JsonResponse(qs_cur.latest('when').response)
                    except:
                        logger.info(f'Errors getting cached results. Ignoring cache, running query ...')
                        pass
 
            # checking request cache to prevent repeated requests, while previous request is still running
            cache_key = set_das_cache(data)

            # log layer requests
            request_log = LayerRequestLog.create_log(data, request_type)

            dlq = DisturbanceLayerQuery(masterlist_questions, geojson, proposal)
            response = dlq.query()
            response['sqs_log_url'] = request.build_absolute_uri().replace('das/spatial_query', f'logs/{request_log.id}/request_log')
            #response['metrics'] = dlq.lq_helper.metrics
            response['request_type'] = request_type
            response['when'] = request_log.when.strftime("%Y-%m-%dT%H:%M:%S")
      
            request_log.response = response
            total_time = round(time.time() - start_time, 3)
            request_log.response.update({
                'metrics': dict(
                    #total_query_time=round(time.time() - start_time, 3),
                    total_query_time=total_time,
                    #total_query_time=round(dlq.lq_helper.total_query_time, 3),
                    spatial_query=dlq.lq_helper.metrics,
                )
            })
            request_log.save()

        except LayerProviderException as e:
            clear_cache(cache_key)
            raise LayerProviderException(str(e))
        except Exception as e:
            clear_cache(cache_key)
            logger.error(traceback.print_exc())
            return JsonResponse(status=status.HTTP_500_INTERNAL_SERVER_ERROR, data={'errors': str(e)})

        cache.delete(cache_key)
        logger.info(f'Propodal ID {proposal["id"]}: Total Time: {total_time} secs')
        return JsonResponse(status=status.HTTP_200_OK, data=response)


class DisturbanceLayerQueueView(View):
    queryset = Layer.objects.filter().order_by('id')

    @csrf_exempt
    def post(self, request):            
        """ Intersect user provided Shapefile/GeoJSON with SQS layers to infer required responses

        import requests
        from sqs.utils.das_tests.request_log.das_query import DAS_QUERY_JSON
        r = requests.post(url=f'http://localhost:8002/api/v1/das/spatial_query/', data={data: json.dumps(DAS_QUERY_JSON)})
        """

        try:
            data = json.loads(request.POST['data'])

            proposal = data.get('proposal')
            current_ts = proposal.get('current_ts') # only available following subsequent Prefill requests
            geojson = data.get('geojson')
            masterlist_questions = data.get('masterlist_questions')
            request_type = data.get('request_type')
            system = data.get('system')
            requester = data.get('requester')

            if proposal is None or proposal.get('schema') is None or proposal.get('id') is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No Proposal schema specified in Request'})
            if geojson is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No Shapefile/GeoJSON found for Proposal {proposal.get("id")}'})
            if len(masterlist_questions)==0:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No CDDP Masterlist Questions specified in Request'})
            if request_type is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No Request_Type specified in Request'})
            if system is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No System Name specified in Request'})
            if requester is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No Request User/Email specified in Request'})

            is_valid = is_valid_schema(data)
            if not is_valid:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'Invalid Schema Payload in Request: {is_valid}'})

            task_qs = Task.objects.filter(
                status=Task.STATUS_RUNNING, system=system, app_id=proposal["id"],
            )
            if task_qs.count() > 0:
                response = {'message': f'Request aborted: Task is already running: Proposal {proposal.get("id")}'}

            request_log = LayerRequestLog.create_log(data, request_type)

            task, created = Task.objects.update_or_create(
                system=system,
                app_id=proposal['id'],
                status=Task.STATUS_CREATED,
                defaults={
                    'description': f'{system}_{request_type}_{proposal["id"]}',
                    'script': 'python manage.py das_intersection_query', 
                    'parameters': '', 
                    #'data': data,
                    'request_log': request_log,
                    'requester': requester,
                },
            )

            if created:
                response = {'data': {'task_id': task.id, 'task_created': created},
                            'message': f'Requested Task is queued at position {task.position}', 'position': f'{task.position}'
                           }
            else: 
                # request is an update to an existing queued task 
                request_log = task.request_log
                request_log.data = data
                request_log.save()

                task.requester = requester
                task.created = datetime.now().replace(tzinfo=timezone.utc)
                task.save()

                response = {'data': {'task_id': task.id, 'task_created': created}, 
                            'message': f'Previously queued request at position {task.position} has been updated with new request', 'position': f'{task.position}'
                           }

        except Exception as e:
            logger.error(traceback.print_exc())
            return JsonResponse(status=status.HTTP_500_INTERNAL_SERVER_ERROR, data={'errors': str(e)})

        return JsonResponse(status=status.HTTP_200_OK, data=response)


class DefaultLayerProviderView(View):
    queryset = Layer.objects.filter().order_by('id')
    """ http://localhost:8002/api/v1/layers.json """

    @csrf_exempt
    def post(self, request, *args, **kwargs):            
        ''' Allows to create/update layer 
                1. sets active, if inactive 
                2. creates/updates layer from Geoserver
        '''
        try:
            layer_details = json.loads(request.POST['layer_details'])
            system = request.POST['system']

            layer_name = layer_details.get('layer_name')
            url = layer_details.get('layer_url')
            logger.info(f'Layer Create/Update request from System {system}: {layer_name} - {url}')

            if layer_name is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No layer_name specified in Request'})
            if url is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No layer url specified in Request'})

            qs_layer = self.queryset.filter(name=layer_name)
            cur_version = qs_layer[0].version if qs_layer.exists() else None
            layer_info, layer_gdf = DbLayerProvider(layer_name, url).get_layer_from_geoserver()

        except LayerProviderException as e:
            logger.error(traceback.print_exc())
#            if 'Layer exceeds max' in str(e):
#                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'GET Request from SQS to Geoserver failed. Layer {layer_name} exceeds max. size of 256MB'})
            return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'GET Request from SQS to Geoserver failed. Check Geoserver/Source if layer/GeoJSON exists. URL: {url}'})

        except Exception as e:
            logger.error(traceback.print_exc())
            return JsonResponse(status=status.HTTP_500_INTERNAL_SERVER_ERROR, data={'errors': traceback.format_exc()})

        if cur_version is None or layer_info.get('layer_version') > cur_version:
            layer_info['message'] = f'Layer {layer_info["layer_name"]} Updated.'
        else:
            layer_info['message'] = f'Layer {layer_info["layer_name"]} Not Updated - there is no change to layer'
        return JsonResponse(layer_info)


class TestView(View):

    @csrf_exempt
    def post(self, request):
        return HttpResponse('This is a POST only view')

    def get(self, request):
        return HttpResponse('This is a GET only view')

#class PointQueryLayerView(View):
#    queryset = Layer.objects.filter().order_by('id')
#
#    @csrf_exempt
#    def post(self, request):            
#        ''' Query layer to determine layer properties give latitude, longitude and layer name
#
#            data = {"layer_name": "cddp:dpaw_regions", "layer_attrs":["office","region"], "longitude": 121.465836, "latitude":-30.748890}
#            r=requests.post('http://localhost:8002/api/v1/point_query', data={'data': json.dumps(data)})
#        '''
#        try:
#            data = json.loads(request.POST['data'])
#
#            layer_name = data['layer_name']
#            longitude = data['longitude']
#            latitude = data['latitude']
#            layer_attrs = data.get('layer_attrs', [])
#            predicate = data.get('predicate', 'within')
#
#            helper = PointQueryHelper(layer_name, layer_attrs, longitude, latitude)
#            response = helper.spatial_join(predicate=predicate)
#        except Exception as e:
#            logger.error(traceback.print_exc())
#            return JsonResponse(status=status.HTTP_500_INTERNAL_SERVER_ERROR, data={'errors': traceback.format_exc()})
#
#        return JsonResponse(response)
#


#class CheckLayerView(View):
#    queryset = Layer.objects.filter().order_by('id')
#    """ http://localhost:8002/api/v1/layers.json """
#
#    @csrf_exempt
#    def post(self, request, *args, **kwargs):            
#        ''' Allows to create/update layer 
#                1. sets active, if inactive 
#                2. creates/updates layer from Geoserver
#        '''
#        try:
#
#            import ipdb; ipdb.set_trace()
#            data = request.POST.get('data')
#            if data is None:
#                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No layer_details specified in Request'})
#
#            data = json.loads(data)
#            if 'layer_name' not in data or data['layer_name]' is None:
#                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No layer_name specified in Request'})
#
#            qs_layer = self.queryset.filter(name=layer_name)
#            if not qs_layer.exists():
#                return  JsonResponse(status=status.HTTP_200_OK, data={'message': f'Layer not available on SQS'})
#
#            timestamp = qs_layer[0].modified_date if qs_layer[0].modified_date else qs_layer[0].created_date
#
#        except Exception as e:
#            logger.error(traceback.print_exc())
#            return JsonResponse(status=status.HTTP_500_INTERNAL_SERVER_ERROR, data={'errors': traceback.format_exc()})
#
#        return  JsonResponse(status=status.HTTP_200_OK, data={'message': f'Layer is available on SQS. Last Updated: {timestamp.strftime("%Y-%m-%d %H:%M:%S")}'})




