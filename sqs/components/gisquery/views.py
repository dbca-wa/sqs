from django.conf import settings
from django.http import Http404, HttpResponse, HttpResponseRedirect, JsonResponse
from rest_framework import status
from http import HTTPStatus
from django.urls import reverse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views import View
from datetime import datetime, timedelta
import requests
import json
import unicodecsv
import pytz
import traceback

from sqs.components.gisquery.models import Layer, LayerRequestLog
from sqs.utils.geoquery_utils import DisturbanceLayerQueryHelper, LayerQuerySingleHelper, PointQueryHelper
from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData
from sqs.utils.loader_utils import LayerLoader
from sqs.decorators import basic_exception_handler, apikey_required

from sqs.components.api import models as api_models
from sqs.components.api import utils as api_utils
from sqs.decorators import ip_check_required, traceback_exception_handler, apiview_response_exception_handler

import logging
logger = logging.getLogger(__name__)


class TestView(View):

    @csrf_exempt
    def post(self, request):
        return HttpResponse('This is a POST only view')

    def get(self, request):
        return HttpResponse('This is a GET only view')


class DisturbanceLayerView(View):
    queryset = Layer.objects.filter().order_by('id')

    @csrf_exempt
    @ip_check_required
    def post(self, request):            
        """ 
        import requests
        from sqs.utils.das_tests.request_log.das_query import DAS_QUERY_JSON
        requests.post('http://localhost:8002/api/v1/das/spatial_query/', json=CDDP_REQUEST_JSON)
        apikey='1234'
        r=requests.post(url=f'http://localhost:8002/api/v1/das/{apikey}/spatial_query/', json=DAS_QUERY_JSON)
        """
        #import ipdb; ipdb.set_trace()
        try:
            data = json.loads(request.POST['data'])

            proposal = data.get('proposal')
            geojson = data.get('geojson')
            masterlist_questions = data.get('masterlist_questions')
            system = data.get('system')

            if proposal is None or proposal.get('schema') is None or proposal.get('id') is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No Proposal schema specified in Request'})
            if geojson is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No Shapefile/GeoJSON found for Proposal {proposal.get("id")}'})
            if len(masterlist_questions)==0:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No CDDP Masterlist Questions specified in Request'})
            if system is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No System Name specified in Request'})

            # log layer requests
            request_log = LayerRequestLog.create_log(data)

            dlq = DisturbanceLayerQuery(masterlist_questions, geojson, proposal)
            response = dlq.query()
      
            request_log.response = response
            request_log.save()
        except Exception as e:
            logger.error(traceback.print_exc())
            return JsonResponse(status=status.HTTP_500_INTERNAL_SERVER_ERROR, data={'errors': traceback.format_exc()})

        return JsonResponse(response)

class PointQueryLayerView(View):
    queryset = Layer.objects.filter().order_by('id')

    @csrf_exempt
    @ip_check_required
    def post(self, request):            
        ''' data = {"layer_name": "cddp:dpaw_regions", "layer_attrs":["office","region"], "longitude": 121.465836, "latitude":-30.748890}
            r=requests.post('http://localhost:8002/api/v1/point_query', data={'data': json.dumps(data)})
        '''
        #import ipdb; ipdb.set_trace()
        try:
            data = json.loads(request.POST['data'])

            layer_name = data['layer_name']
            longitude = data['longitude']
            latitude = data['latitude']
            layer_attrs = data.get('layer_attrs', [])
            predicate = data.get('predicate', 'within')

            helper = PointQueryHelper(layer_name, layer_attrs, longitude, latitude)
            response = helper.spatial_join(predicate=predicate)
        except Exception as e:
            logger.error(traceback.print_exc())
            return JsonResponse(status=status.HTTP_500_INTERNAL_SERVER_ERROR, data={'errors': traceback.format_exc()})

        return JsonResponse(response)



