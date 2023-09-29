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
from sqs.utils.loader_utils import LayerLoader, DbLayerProvider
from sqs.components.gisquery.serializers import (
    #DisturbanceLayerSerializer,
    DefaultLayerSerializer,
    GeoJSONLayerSerializer,
    LayerRequestLogSerializer,
)
from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData
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

            List Layers:
            http://localhost:8002/api/v1/layers/

            List Details Specific Layer:
            http://localhost:8002/api/v1/layers/378/
        """
        pk = kwargs.get('pk')
        if pk == 'last':
            instance = self.queryset.last()
        else:
            instance = self.get_object()

        serializer = self.get_serializer(instance) 
        return Response(serializer.data)

    @action(detail=True, methods=['GET',])
    @traceback_exception_handler
    def geojson(self, request, *args, **kwargs):            
        """ 
        http://localhost:8002/api/v1/layers/informal_reservess/geojson.json

        # List all layers available on SQS
        http://localhost:8002/api/v1/layers/
        """
        self.serializer_class = GeoJSONLayerSerializer
        layer_name = kwargs.get('pk')

        # get from cache, if exists. Otherwise get from DB, if exists
        layer_info, layer_gdf = DbLayerProvider(layer_name=layer_name, url='').get_layer(from_geoserver=False)

        if layer_gdf is None:
            return  JsonResponse(
                status=status.HTTP_400_BAD_REQUEST, 
                data={'errors': f'Layer Name {layer_name} Not Found. Check list of layers available from URL \'{request.META["HTTP_HOST"]}/api/v1/layers/\''}
            )

        return Response(json.loads(layer_gdf.to_json()))

    @action(detail=False, methods=['GET',])
    @traceback_exception_handler
    def check_layer(self, request, *args, **kwargs):            
        """ http://localhost:8002/api/v1/layers/check_layer/?layer_name=informal_reserves
            requests.get('http://localhost:8002/api/v1/layers/check_sqs_layer', params={'layer_name':'informal_reserves'})

        Check if layer is loaded and is available on SQS
        """
        layer_name = request.GET.get('layer_name')

        if layer_name is None:
            return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No layer_name specified in Request'})

        qs_layer = self.queryset.filter(name=layer_name)
        if not qs_layer.exists():
            return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'Layer not available on SQS'})

        timestamp = qs_layer[0].modified_date if qs_layer[0].modified_date else qs_layer[0].created_date
        return  JsonResponse(status=status.HTTP_200_OK, data={'message': f'Layer is available on SQS. Last Updated: {timestamp.strftime("%Y-%m-%d %H:%M:%S")}'})

    @action(detail=False, methods=['GET',])
    @traceback_exception_handler
    def get_attributes(self, request, *args, **kwargs):            
        """ http://localhost:8002/api/v1/layers/get_attributes/?layer_name=informal_reserves
            http://localhost:8002/api/v1/layers/get_attributes/?layer_name=CPT_LOCAL_GOVT_AREAS&attr_name=LGA_TYPE
            http://localhost:8002/api/v1/layers/get_attributes/?layer_name=CPT_LOCAL_GOVT_AREAS&attrs_only=true
            http://localhost:8002/api/v1/layers/get_attributes/?layer_name=CPT_LOCAL_GOVT_AREAS&use_cache=False

            requests.get('http://localhost:8002/api/v1/layers/get_attributes', params={'layer_name':'informal_reserves'})

            List Layers:
            http://localhost:8002/api/v1/layers/

        Check if layer is loaded and is available on SQS
        """
        layer_name = request.GET.get('layer_name')
        attrs_only = request.GET.get('attrs_only')
        attr_name = request.GET.get('attr_name')
        use_cache = request.GET.get('use_cache')

        if layer_name is None:
            return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No layer_name specified in Request'})


        if use_cache and use_cache.lower()=='false':
            qs_layer = self.queryset.filter(name=layer_name)
            if not qs_layer.exists():
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'Layer not available on SQS'})
            layer_gdf = qs_layer[0].to_gdf
        else:
            # get from cache, if exists. Otherwise get from DB, if exists
            layer_info, layer_gdf = DbLayerProvider(layer_name=layer_name, url='').get_layer(from_geoserver=False)

            if layer_gdf is None:
                return  JsonResponse(
                    status=status.HTTP_400_BAD_REQUEST, 
                    data={'errors': f'Layer Name {layer_name} Not Found. Check list of layers available from URL \'{request.META["HTTP_HOST"]}/api/v1/layers/\''}
                )

        filtered_cols = layer_gdf.loc[:, layer_gdf.columns != 'geometry'].columns # exclude column 'goeometry'
        if attrs_only:
            return  Response(dict(
                    layer_name=layer_name,
                    attributes=filtered_cols
                )
            )
           
        if attr_name and attr_name.lower() in filtered_cols.str.lower():
            filtered_cols = [attr_name.strip()]
        elif attr_name:
            return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'Layer attribute {attr_name} not available on SQS. Available attrs: {filtered_cols}'})

        data=[]
        for col in filtered_cols:
            if col.lower() != 'geometry':
                data.append(dict(attribute=col, values=list(layer_gdf[col].unique())))

        return  Response(data)

    @action(detail=True, methods=['GET',])
    @traceback_exception_handler
    def clear_cache(self, request, *args, **kwargs):            
        """ http://localhost:8002/api/v1/layers/CPT_LOCAL_GOVT_AREAS/clear_cache/

            List Layers:
            http://localhost:8002/api/v1/layers/

        Clears layer cache, if exists on SQS
        """
        layer_name = kwargs.get('pk')

        layer_provider = DbLayerProvider(layer_name=layer_name, url='')
        layer_info, layer_gdf = layer_provider.get_from_cache()
        if layer_info:
            layer_provider.clear_cache()
            return  JsonResponse({'message': f'Cache cleared: {layer_name}'})

        return  JsonResponse(data={'error': f'Cache not found: {layer_name}'}, status=status.HTTP_400_BAD_REQUEST)

            
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
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs/<proposal_id>/request_data?request_type=all&system=das ('all'/'partial'/'single')
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs/<proposal_id>/request_data?request_type=all&system=das&when=True

            if '&when=True' is provided only timestamp details will be returned in the response
        """
        app_id = kwargs.get('pk')
        system = request.GET['system']
        request_type = request.GET.get('request_type', 'FULL')
        when = request.GET.get('when')

        qs = self.queryset.filter(app_id=app_id, request_type=request_type.upper(), system=system.upper())
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


class PointQueryViewSet(viewsets.ModelViewSet):
    queryset = Layer.objects.filter().order_by('id')
    serializer_class = DefaultLayerSerializer
    http_method_names = ['get'] #, 'post', 'patch', 'delete']

    def list(self, request, *args, **kwargs):            
        return Response(status.HTTP_405_METHOD_NOT_ALLOWED)

    @action(detail=False, methods=['GET',])
    @traceback_exception_handler
    def lonlat_attrs(self, request, *args, **kwargs):            
        ''' Query layer to determine layer properties give latitude, longitude and layer name

            payload = (('layer_name', 'cddp:dpaw_regions'), ('layer_attrs', 'office, region'), ('lon', 121.465836), ('lat',-30.748890))
            r=requests.get('http://localhost:8002/api/v1/point_query/lonlat_attrs', params=payload)

            https://sqs-dev.dbca.wa.gov.au/api/v1/point_query/lonlat_attrs?layer_name=cddp:dpaw_regions&layer_attrs=office,region&lon=121.465836&lat=-30.748890
        '''
        try:
            layer_name = request.GET['layer_name']
            layer_attrs = request.GET.get('layer_attrs', [])
            longitude = request.GET['lon']
            latitude = request.GET['lat']
            predicate = request.GET.get('predicate', 'within')

            if isinstance(layer_attrs, str):
                layer_attrs = [i.strip() for i in layer_attrs.split(',')]

            helper = PointQueryHelper(layer_name, layer_attrs, longitude, latitude)
            response = helper.spatial_join(predicate=predicate)
        except Exception as e:
            logger.error(traceback.print_exc())
            return JsonResponse(status=status.HTTP_500_INTERNAL_SERVER_ERROR, data={'errors': traceback.format_exc()})

        return JsonResponse(status=response.get('status'), data=response)


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


