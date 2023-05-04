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

from sqs.components.gisquery.models import Layer, LayerRequestLog
from sqs.utils.geoquery_utils import DisturbanceLayerQueryHelper, LayerQuerySingleHelper, PointQueryHelper
from sqs.utils.loader_utils import LayerLoader
from sqs.components.gisquery.serializers import (
    GeoTestSerializer,
    #FeatureGeometrySerializer,
    DisturbanceLayerSerializer,
    DefaultLayerSerializer,
)
from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData
from sqs.utils.loader_utils import LayerLoader
from sqs.decorators import basic_exception_handler, ip_check_required

from sqs.components.api import models as api_models
from sqs.components.api import utils as api_utils

from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view

import logging
logger = logging.getLogger(__name__)

from rest_framework.permissions import AllowAny


#@method_decorator(csrf_exempt, name='dispatch')
#class DisturbanceLayerAPIView(views.APIView):
#    queryset = Layer.objects.filter().order_by('id')
#    serializer_class = DisturbanceLayerSerializer
#    permission_classes = [AllowAny]
#
#    def get(self, request, *args, **kwargs):            
#        #import ipdb; ipdb.set_trace()
#        return Response({"test":"get_test"})
#
#    def post(self, request, *args, **kwargs):            
#        """ http://localhost:8002/api/v1/layers/<APIKEY>/1/layer.json 
#            https://sqs-dev.dbca.wa.gov.au/api/v1/layers/<APIKEY>/1/layer.json
#        """
#        #import ipdb; ipdb.set_trace()
#        return Response({"test":"post_test"})


class DisturbanceLayerViewSet(viewsets.ModelViewSet):
    queryset = Layer.objects.filter().order_by('id')
    serializer_class = DisturbanceLayerSerializer

    @action(detail=False, methods=['GET',])
    def csrf_token(self, request, *args, **kwargs):            
        """ https://sqs-dev.dbca.wa.gov.au/api/v1/das/csrf_token.json
            https://sqs-dev.dbca.wa.gov.au/api/v1/das/<APIKEY>/csrf_token.json
        """
        return Response({"test":"get_test"})

    @action(detail=False, methods=['POST',]) # POST because request will contain GeoJSON polygon to intersect with layer stored on SQS. If layer does not exist, SQS will retrieve from KMI
    @ip_check_required
    def spatial_query(self, request, *args, **kwargs):            
        """ 
        import requests
        from sqs.utils.das_tests.request_log.das_query import DAS_QUERY_JSON
        requests.post('http://localhost:8002/api/v1/das/spatial_query/', json=CDDP_REQUEST_JSON)
        apikey='1234'
        r=requests.post(url=f'http://localhost:8002/api/v1/das/{apikey}/spatial_query/', json=DAS_QUERY_JSON)

        OR
        curl -d @sqs/utils/das_tests/request_log/das_curl_query.json -X GET http://localhost:8002/api/v1/das/spatial_query/ --header "Content-Type: application/json" --header "Accept: application/json"
        """
        #import ipdb; ipdb.set_trace()
        masterlist_questions = request.data['masterlist_questions']
        geojson = request.data['geojson']
        proposal = request.data['proposal']
        system = proposal.get('system', 'DAS')

        # log layer requests
        request_log = LayerRequestLog.create_log(request.data)

        dlq = DisturbanceLayerQuery(masterlist_questions, geojson, proposal)
        response = dlq.query()
  
        request_log.response = response
        request_log.save()

        return Response(response)

#class DefaultLayerViewSet(viewsets.GenericViewSet):
class DefaultLayerViewSet(viewsets.ModelViewSet):
    """ http://localhost:8002/api/v1/<APIKEY>/layers.json """
    queryset = Layer.objects.filter().order_by('id')
    serializer_class = DefaultLayerSerializer

    @action(detail=False, methods=['GET',])
    def csrf_token(self, request, *args, **kwargs):            
        """ https://sqs-dev.dbca.wa.gov.au/api/v1/layers/1/csrf_token.json
            https://sqs-dev.dbca.wa.gov.au/api/v1/layers/<APIKEY>/1/csrf_token.json
        """
        return Response({"test":"get_test"})

    @action(detail=False, methods=['POST',])
    @ip_check_required
    def add_layer(self, request, *args, **kwargs):            
        """ 
        curl -d @sqs/data/json/threatened_priority_flora.json -X POST http://localhost:8002/api/v1/layers/<APIKEY>/add_layer.json --header "Content-Type: application/json" --header "Accept: application/json"
        """
        layer_name = request.data['layer_name']
        url = request.data['url']
        geojson = request.data['geojson']

        loader = LayerLoader(url, layer_name)
        layer = loader.load_layer(geojson=geojson)
        return Response(**loader.data)

    @action(detail=True, methods=['GET',])
    @ip_check_required
    def layer(self, request, *args, **kwargs):            
        """ http://localhost:8002/api/v1/layers/<APIKEY>/1/layer.json 
            https://sqs-dev.dbca.wa.gov.au/api/v1/layers/<APIKEY>/1/layer.json
        """
        #import ipdb; ipdb.set_trace()
        instance = self.get_object()
        serializer = self.get_serializer(instance) 
        return Response(serializer.data)

    @action(detail=True, methods=['POST',])
    def layer_test(self, request, *args, **kwargs):            
        """ http://localhost:8002/api/v1/layers/<APIKEY>/1/layer.json 
            https://sqs-dev.dbca.wa.gov.au/api/v1/layers/1/layer_test.json
            https://sqs-dev.dbca.wa.gov.au/api/v1/layers/<APIKEY>/1/layer_test.json
        """
        return Response({"test":"test"})


    @action(detail=False, methods=['POST',])
    @ip_check_required
    def point_query(self, request, *args, **kwargs):            
        """ 
        http://localhost:8002/api/v1/layers/<APIKEY>/point_query.json

        curl -d '{"layer_name": "cddp:dpaw_regions", "layer_attrs":["office","region"], "longitude": 121.465836, "latitude":-30.748890}' -X POST http://localhost:8002/api/v1/layers/point_query.json --header "Content-Type: application/json" --header "Accept: application/json"
        """

        #import ipdb; ipdb.set_trace()
        layer_name = request.data['layer_name']
        longitude = request.data['longitude']
        latitude = request.data['latitude']
        layer_attrs = request.data.get('layer_attrs', [])
        predicate = request.data.get('predicate', 'within')

        helper = PointQueryHelper(layer_name, layer_attrs, longitude, latitude)
        response = helper.spatial_join(predicate=predicate)
        return Response(response)


