from django.conf import settings
from django.urls import reverse
from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views import View
from datetime import datetime, timedelta
import requests
import json
import unicodecsv
import pytz

from django.http import Http404, HttpResponse, HttpResponseRedirect, JsonResponse
from django.conf import settings
from django.urls import reverse

from sqs.components.gisquery.models import Layer, LayerRequestLog
from sqs.utils.geoquery_utils import DisturbanceLayerQueryHelper, LayerQuerySingleHelper, PointQueryHelper
from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData
from sqs.utils.loader_utils import LayerLoader
from sqs.decorators import basic_exception_handler, apikey_required

from sqs.components.api import models as api_models
from sqs.components.api import utils as api_utils

from rest_framework.response import Response

import logging
logger = logging.getLogger(__name__)


class TestView(View):

    @csrf_exempt
    def post(self, request):
        return HttpResponse('This is a POST only view')

    def get(self, request):
        return HttpResponse('This is a GET only view')


#class DisturbanceLayerView(View):
#    queryset = Layer.objects.filter().order_by('id')
#
#    @csrf_exempt
#    def post(self, request):            
#        """ 
#        import requests
#        from sqs.utils.das_tests.request_log.das_query import DAS_QUERY_JSON
#        requests.post('http://localhost:8002/api/v1/das/spatial_query/', json=CDDP_REQUEST_JSON)
#        apikey='1234'
#        r=requests.post(url=f'http://localhost:8002/api/v1/das/{apikey}/spatial_query/', json=DAS_QUERY_JSON)
#        """
#        masterlist_questions = request.data['masterlist_questions']
#        geojson = request.data['geojson']
#        proposal = request.data['proposal']
#        system = proposal.get('system', 'DAS')
#
#        # log layer requests
#        request_log = LayerRequestLog.create_log(request.data)
#
#        dlq = DisturbanceLayerQuery(masterlist_questions, geojson, proposal)
#        response = dlq.query()
#  
#        request_log.response = response
#        request_log.save()
#
#        return Response(response)


