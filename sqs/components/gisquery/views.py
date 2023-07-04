from django.conf import settings
from django.http import Http404, HttpResponse, HttpResponseRedirect, JsonResponse
from rest_framework import status
from http import HTTPStatus
from django.urls import reverse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views import View
from django.core.exceptions import ObjectDoesNotExist
from datetime import datetime, timedelta
import requests
import json
import unicodecsv
import pytz
import traceback

from sqs.components.gisquery.models import Layer, LayerRequestLog
from sqs.utils.geoquery_utils import DisturbanceLayerQueryHelper, LayerQuerySingleHelper, PointQueryHelper
from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData
from sqs.utils.loader_utils import DbLayerProvider

from sqs.components.api import models as api_models
from sqs.components.api import utils as api_utils
from sqs.decorators import ip_check_required, basic_exception_handler, traceback_exception_handler, apiview_response_exception_handler
from sqs.exceptions import LayerProviderException

import logging
logger = logging.getLogger(__name__)


class DisturbanceLayerView(View):
    queryset = Layer.objects.filter().order_by('id')

    @csrf_exempt
    def post(self, request):            
        """ Intersect user provided Shapefile/GeoJSON with SQS layers to infer required responses

        import requests
        from sqs.utils.das_tests.request_log.das_query import DAS_QUERY_JSON
        requests.post('http://localhost:8002/api/v1/das/spatial_query/', json=CDDP_REQUEST_JSON)
        r=requests.post(url=f'http://localhost:8002/api/v1/das/spatial_query/', json=DAS_QUERY_JSON)
        """
        try:
            data = json.loads(request.POST['data'])

            proposal = data.get('proposal')
            current_ts = proposal.get('current_ts')
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
            if system is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No System Name specified in Request'})

            # check if a previous request exists with a more recent timestamp
            # datetime.strptime('2023-07-04T10:53:17', '%Y-%m-%dT%H:%M:%S')
            qs_cur = LayerRequestLog.objects.filter(app_id=proposal['id'], request_type='ALL', system='DAS')
            if qs_cur.exists() and current_ts is not None:
                ts = datetime.strptime(current_ts, '%Y-%m-%dT%H:%M:%S')
                if qs_cur.latest('when').when > ts:
                    return JsonResponse(qs_cur.latest('when').response)

            # log layer requests
            request_log = LayerRequestLog.create_log(data, request_type)

            dlq = DisturbanceLayerQuery(masterlist_questions, geojson, proposal)
            response = dlq.query()
            response['sqs_log_url'] = request.build_absolute_uri().replace('das/spatial_query', f'logs/{request_log.id}/request_log')
            response['request_type'] = request_type
            response['when'] = request_log.when.strftime("%Y-%m-%dT%H:%M:%S")
      
            #request_log.request_type = request_type
            request_log.response = response
            request_log.save()

        except LayerProviderException as e:
            raise LayerProviderException(str(e))
        except Exception as e:
            logger.error(traceback.print_exc())
            return JsonResponse(status=status.HTTP_500_INTERNAL_SERVER_ERROR, data={'errors': traceback.format_exc()})

        return JsonResponse(response)


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
            #import ipdb; ipdb.set_trace()
            layer_details = json.loads(request.POST['layer_details'])

            layer_name = layer_details.get('layer_name')
            url = layer_details.get('layer_url')

            if layer_name is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No layer_name specified in Request'})
            if url is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No layer url specified in Request'})

            qs_layer = self.queryset.filter(name=layer_name)
            if qs_layer.exists():
                cur_version = qs_layer[0].version
            layer_info, layer_gdf = DbLayerProvider(layer_name, url).get_layer_from_geoserver()

            #if layer_info.get('layer_version') > cur_version:
        

        except LayerProviderException as e:
            logger.error(traceback.print_exc())
            return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'GET Request from SQS to Geoserver failed. Check Geoserver if layer/GeoJSON exists. URL: {url}'})

        except Exception as e:
            logger.error(traceback.print_exc())
            return JsonResponse(status=status.HTTP_500_INTERNAL_SERVER_ERROR, data={'errors': traceback.format_exc()})

        layer_info['message'] = f'Layer {layer_info["layer_name"]} Updated.'
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




