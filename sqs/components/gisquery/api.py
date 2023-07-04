from django.http import Http404, HttpResponse, HttpResponseRedirect, JsonResponse
from django.conf import settings
from django.db import transaction
from django.urls import reverse
from django.core.exceptions import ValidationError
from django.db.models import Q

from wsgiref.util import FileWrapper
from rest_framework import viewsets, serializers, status, generics, views
#from rest_framework.decorators import detail_route, list_route, renderer_classes, parser_classes
from rest_framework.decorators import action, renderer_classes, parser_classes
from rest_framework.response import Response
from rest_framework.renderers import JSONRenderer
from rest_framework.permissions import IsAuthenticated, AllowAny, IsAdminUser, BasePermission
from rest_framework.pagination import PageNumberPagination
import traceback
import json
from datetime import datetime

from sqs.components.gisquery.models import Layer, LayerRequestLog
from sqs.utils.geoquery_utils import DisturbanceLayerQueryHelper, LayerQuerySingleHelper, PointQueryHelper
from sqs.utils.loader_utils import LayerLoader
from sqs.components.gisquery.serializers import (
    #DisturbanceLayerSerializer,
    DefaultLayerSerializer,
    LayerRequestLogSerializer,
)
from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData
from sqs.utils.loader_utils import LayerLoader
from sqs.decorators import basic_exception_handler, ip_check_required, traceback_exception_handler

from sqs.components.api import models as api_models
from sqs.components.api import utils as api_utils

from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view

import logging
logger = logging.getLogger(__name__)

from rest_framework.permissions import AllowAny

class DefaultLayerViewSet(viewsets.ModelViewSet):
    """ http://localhost:8002/api/v1/<APIKEY>/layers.json """
    queryset = Layer.objects.filter().order_by('id')
    serializer_class = DefaultLayerSerializer
    http_method_names = ['get']

    @action(detail=False, methods=['GET',])
    @traceback_exception_handler
    def csrf_token(self, request, *args, **kwargs):            
        """ https://sqs-dev.dbca.wa.gov.au/api/v1/layers/1/csrf_token.json
            https://sqs-dev.dbca.wa.gov.au/api/v1/layers/1/csrf_token.json
        """
        return Response({"test":"get_test"})

    @action(detail=True, methods=['GET',])
    @traceback_exception_handler
    def layer(self, request, *args, **kwargs):            
        """ http://localhost:8002/api/v1/layers/1/layer.json 
            https://sqs-dev.dbca.wa.gov.au/api/v1/layers/last/layer.json
        """
        #import ipdb; ipdb.set_trace()
        pk = kwargs.get('pk')
        if pk == 'last':
            instance = self.queryset.last()
        else:
            instance = self.get_object()

        serializer = self.get_serializer(instance) 
        return Response(serializer.data)

    @action(detail=False, methods=['GET',])
    @traceback_exception_handler
    def check_layer(self, request, *args, **kwargs):            
        """ http://localhost:8002/api/v1/layers/check_sqs_layer 
            requests.get('http://localhost:8002/api/v1/layers/check_sqs_layer', params={'layer_name':'cddp:dpaw_regions'})

        Check if layer is loaded and is available on SQS
        """
        #import ipdb; ipdb.set_trace()
        layer_name = request.GET.get('layer_name')

        if layer_name is None:
            return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No layer_name specified in Request'})

        qs_layer = self.queryset.filter(name=layer_name)
        if not qs_layer.exists():
            return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'Layer not available on SQS'})

        timestamp = qs_layer[0].modified_date if qs_layer[0].modified_date else qs_layer[0].created_date
        return  JsonResponse(status=status.HTTP_200_OK, data={'message': f'Layer is available on SQS. Last Updated: {timestamp.strftime("%Y-%m-%d %H:%M:%S")}'})

class LayerRequestLogViewSet(viewsets.ModelViewSet):
    queryset = LayerRequestLog.objects.filter().order_by('id')
    serializer_class = LayerRequestLogSerializer
    http_method_names = ['get'] #, 'post', 'patch', 'delete']

    @traceback_exception_handler
    def list(self, request, *args, **kwargs):            
        """ http://localhost:8002/api/v1/logs/
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs/
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs?records=5
        """
        records = self.request.GET.get('records', 20)
        queryset = self.queryset.all().order_by('-pk')[:int(records)]
        serializer = self.get_serializer(queryset, many=True, remove_fields=['data', 'response'])
        return Response(serializer.data)

    @action(detail=True, methods=['GET',])
    @traceback_exception_handler
    def request_data(self, request, *args, **kwargs):            
        """
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs/<proposal_id>/request_data
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs/<proposal_id>/request_data?request_type=all ('all'/'partial'/'single')
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs/<proposal_id>/request_data?request_type=all&when=True

            if '&when=True' is provided only timestamp details will be returned in the response
        """
        #import ipdb; ipdb.set_trace()
        proposal_id = kwargs.get('pk')
        request_type = request.GET.get('request_type', 'ALL')
        when = request.GET.get('when')

        qs = self.queryset.filter(app_id=proposal_id, request_type=request_type.upper())
        if not qs.exists():
            return Response(status.HTTP_400_BAD_REQUEST)

        instance = qs.latest('when')

        remove_fields = ['data', 'response'] if when is not None else ['data']
        serializer = self.get_serializer(instance, remove_fields=remove_fields) 
        return Response(serializer.data)

    @action(detail=True, methods=['GET',])
    @traceback_exception_handler
    def request_log(self, request, *args, **kwargs):            
        """ http://localhost:8002/api/v1/logs/766/request_log.json
            https://sqs-dev.dbcachema,wa.gov.au/api/v1/logs/766/request_log.json
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs/last/request_log.json
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs/766/request_log?request_type=all ('all'/'partial'/'single')
        """
        #import ipdb; ipdb.set_trace()
        pk = kwargs.get('pk')
        request_type = request.GET.get('request_type')

        if pk == 'last':
            instance = self.queryset.last()
        elif request_type is not None:
            qs = self.queryset.filter(id=pk, request_type=request_type.upper())
            if not qs.exists():
                return Response(status.HTTP_400_BAD_REQUEST)
            instance = qs[0]
        else:
            instance = self.get_object()

        serializer = self.get_serializer(instance, remove_fields=['data']) 
        return Response(serializer.data)

    @action(detail=True, methods=['GET',])
    @traceback_exception_handler
    def request_log_all(self, request, *args, **kwargs):            
        """ http://localhost:8002/api/v1/logs/766/request_log_all
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs/766/request_log_all
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs/last/request_log_all
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs/766/request_log_all?request_type=all ('all'/'partial'/'single')
        """
        #import ipdb; ipdb.set_trace()
        pk = kwargs.get('pk')
        request_type = request.GET.get('request_type')

        if pk == 'last':
            instance = self.queryset.last()
        elif request_type is not None:
            qs = self.queryset.filter(id=pk, request_type=request_type.upper())
            if not qs.exists():
                return Response(status.HTTP_400_BAD_REQUEST)
            instance = qs[0]
        else:
            instance = self.get_object()

        serializer = self.get_serializer(instance, remove_fields=[]) 
        return Response(serializer.data)



#    @action(detail=False, methods=['POST',])
#    @ip_check_required
#    @traceback_exception_handler
#    def add_layer(self, request, *args, **kwargs):            
#        """ 
#        curl -d @sqs/data/json/threatened_priority_flora.json -X POST http://localhost:8002/api/v1/layers/<APIKEY>/add_layer.json --header "Content-Type: application/json" --header "Accept: application/json"
#        """
#        layer_name = request.data['layer_name']
#        url = request.data['url']
#        geojson = request.data['geojson']
#
#        loader = LayerLoader(url, layer_name)
#        layer = loader.load_layer(geojson=geojson)
#        return Response(**loader.data)

#    @action(detail=True, methods=['POST',])
#    @traceback_exception_handler
#    def layer_test(self, request, *args, **kwargs):            
#        """ http://localhost:8002/api/v1/layers/<APIKEY>/1/layer.json 
#            https://sqs-dev.dbca.wa.gov.au/api/v1/layers/1/layer_test.json
#            https://sqs-dev.dbca.wa.gov.au/api/v1/layers/<APIKEY>/1/layer_test.json
#        """
#        return Response({"test":"test"})


#class DisturbanceLayerViewSet(viewsets.ModelViewSet):
#    queryset = Layer.objects.filter().order_by('id')
#    serializer_class = DisturbanceLayerSerializer
#
#    @action(detail=False, methods=['GET',])
#    @traceback_exception_handler
#    def csrf_token(self, request, *args, **kwargs):            
#        """ https://sqs-dev.dbca.wa.gov.au/api/v1/das/csrf_token.json
#            https://sqs-dev.dbca.wa.gov.au/api/v1/das/<APIKEY>/csrf_token.json
#        """
#        return Response({"test":"get_test"})
#
#    @action(detail=False, methods=['POST',]) # POST because request will contain GeoJSON polygon to intersect with layer stored on SQS. If layer does not exist, SQS will retrieve from KMI
#    @ip_check_required
#    @traceback_exception_handler
#    def spatial_query(self, request, *args, **kwargs):            
#        """ 
#        import requests
#        from sqs.utils.das_tests.request_log.das_query import DAS_QUERY_JSON
#        requests.post('http://localhost:8002/api/v1/das/spatial_query/', json=CDDP_REQUEST_JSON)
#        apikey='1234'
#        r=requests.post(url=f'http://localhost:8002/api/v1/das/{apikey}/spatial_query/', json=DAS_QUERY_JSON)
#
#        OR
#        curl -d @sqs/utils/das_tests/request_log/das_curl_query.json -X GET http://localhost:8002/api/v1/das/spatial_query/ --header "Content-Type: application/json" --header "Accept: application/json"
#        """
#        #import ipdb; ipdb.set_trace()
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

#class DefaultLayerViewSet(viewsets.GenericViewSet):

#    @action(detail=False, methods=['POST',])
#    @ip_check_required
#    @traceback_exception_handler
#    def point_query(self, request, *args, **kwargs):            
#        """ 
#        http://localhost:8002/api/v1/layers/<APIKEY>/point_query.json
#
#        curl -d '{"layer_name": "cddp:dpaw_regions", "layer_attrs":["office","region"], "longitude": 121.465836, "latitude":-30.748890}' -X POST http://localhost:8002/api/v1/layers/point_query.json --header "Content-Type: application/json" --header "Accept: application/json"
#        """
#
#        #import ipdb; ipdb.set_trace()
#        layer_name = request.data['layer_name']
#        longitude = request.data['longitude']
#        latitude = request.data['latitude']
#        layer_attrs = request.data.get('layer_attrs', [])
#        predicate = request.data.get('predicate', 'within')
#
#        helper = PointQueryHelper(layer_name, layer_attrs, longitude, latitude)
#        response = helper.spatial_join(predicate=predicate)
#        return Response(response)


