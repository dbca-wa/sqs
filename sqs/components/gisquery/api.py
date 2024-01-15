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

from sqs.components.gisquery.models import Layer, LayerRequestLog, Task
from sqs.utils.geoquery_utils import DisturbanceLayerQueryHelper, PointQueryHelper
from sqs.utils.loader_utils import LayerLoader, DbLayerProvider
from sqs.components.gisquery.serializers import (
    #DisturbanceLayerSerializer,
    DefaultLayerSerializer,
    GeoJSONLayerSerializer,
    LayerRequestLogSerializer,
    TaskSerializer,
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
        """ https://localhost:8002/api/v1/layers/1/csrf_token.json
        """
        return Response({"test":"get_test"})

    @traceback_exception_handler
    def list(self, request, *args, **kwargs):            
        """ http://localhost:8002/api/v1/layers/
        """
        return Response(self.queryset.values('name', 'url', 'active'))

    @action(detail=True, methods=['GET',])
    @traceback_exception_handler
    def layer(self, request, *args, **kwargs):            
        """ http://localhost:8002/api/v1/layers/1/layer.json 

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
        layer_provider = DbLayerProvider(layer_name=layer_name, url='')
        layer_info, layer_gdf = layer_provider.get_layer(from_geoserver=False)

        if layer_gdf is None:
            return  JsonResponse(
                status=status.HTTP_400_BAD_REQUEST, 
                data={'errors': f'Layer Name {layer_name} Not Found'}
            )

        #return Response(json.loads(layer_gdf.to_json()))
        return Response(layer_provider.layer_geojson)

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
                    #data={'errors': f'Layer Name {layer_name} Not Found in SQS. Check list of layers available from URL \'{request.META["HTTP_HOST"]}/api/v1/layers/\''}
                    data={'errors': f'Layer Name {layer_name} Not Found'}
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
            return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'Attribute {attr_name} not found in {layer_name}.<br><br>Available attrs:<br>{list(filtered_cols)}'})

        data=[]
        for col in filtered_cols:
            if col.lower() != 'geometry':
                data.append(dict(attribute=col, values=layer_gdf[col].dropna().unique().tolist()))

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

    @action(detail=False, methods=['GET',])
    @traceback_exception_handler
    def check_queue(self, request, *args, **kwargs):            
        """ Get status of given job

            http://localhost:8002/api/v1/layers/check_queue/?proposal_id=1780&system=DAS&request_type=FULL
            requests.get('http://localhost:8002/api/v1/layers/check_queue', params={'proposal_id':'1780', 'system':'DAS', 'request_type':'FULL'})
        """

        response = {}
        try:
            proposal_id = request.GET.get('proposal_id')
            request_type = request.GET.get('request_type')
            system = request.GET.get('system')

            if proposal_id is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No Proposal ID specified in Request'})
            if request_type is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No Request_Type specified in Request'})
            if system is None:
                return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': f'No System Name specified in Request'})

            description = f'{system}_{request_type}_{proposal_id}'
            task_qs = Task.objects.filter(
                status__in=[Task.STATUS_CREATED, Task.STATUS_RUNNING], system=system, description=description,
            )
            if task_qs.count() == 0:
                response = {'message': f'Requested Task not found in Queue: {description}'}
            else:
                task = task_qs[0]
                if task.status == Task.STATUS_CREATED:
                    response = {'message': f'Requested Task is queued at position {task.position}'}
                else:
                    response = {'message': f'Requested Task is currently running: {description}'}

        except Exception as e:
            logger.error(traceback.print_exc())
            return JsonResponse(status=status.HTTP_500_INTERNAL_SERVER_ERROR, data={'errors': str(e)})

        return JsonResponse(response)

   
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
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs/<proposal_id>/request_data?request_type=full&system=das ('full'/'partial'/'single')
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs/<proposal_id>/request_data?request_type=full&system=das&when=True

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
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs/766/request_log?request_type=FULL ('full'/'partial'/'single')
            https://sqs-dev.dbca.wa.gov.au/api/v1/logs/766/request_log?metrics=true
        """
        pk = kwargs.get('pk')
        request_type = request.GET.get('request_type')
        metrics = request.GET.get('metrics')

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
        if metrics and metrics == 'true':
            return Response(serializer.data['response']['metrics']['spatial_query'])

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

class TaskViewSet(viewsets.ModelViewSet):
    queryset = Task.objects.filter().order_by('id')
    serializer_class = TaskSerializer
    http_method_names = ['get'] #, 'post', 'patch', 'delete']

#    @traceback_exception_handler
#    def list(self, request, *args, **kwargs):            
#        """ http://localhost:8002/api/v1/tasks/
#            https://sqs-dev.dbca.wa.gov.au/api/v1/tasks/
#            https://sqs-dev.dbca.wa.gov.au/api/v1/tasks?records=5
#        """
#        #import ipdb; ipdb.set_trace()
#        records = self.request.GET.get('records', 20)
#        queryset = self.queryset.all().order_by('-pk')[:int(records)]
#        serializer = self.get_serializer(queryset, many=True) #, remove_fields=['data', 'response'])
#        return Response(serializer.data)

    @action(detail=False, methods=['GET',])
    @traceback_exception_handler
    def get_tasks(self, request, *args, **kwargs):            
        """ http://localhost:8002/api/v1/tasks/get_tasks?task_ids=10,11
            http://localhost:8002/api/v1/tasks/get_tasks?task_ids=10,11&all
        """
        #import ipdb; ipdb.set_trace()
        all_fields = True if 'all' in request.GET else False
        task_ids = [task_id.strip() for task_id in request.GET['task_ids'].split(',')]
        qs = Task.objects.filter(id__in=task_ids)

        remove_fields = [] if all_fields else ['stdout', 'stderr']
        serializer = self.get_serializer(qs, many=True, remove_fields=remove_fields) 
        return Response(serializer.data)


from rest_framework.pagination import PageNumberPagination
from rest_framework_datatables.pagination import DatatablesPageNumberPagination
from rest_framework_datatables.filters import DatatablesFilterBackend
from rest_framework_datatables.renderers import DatatablesRenderer

class TaskPaginatedViewSet(viewsets.ModelViewSet):
    #filter_backends = (ProposalFilterBackend,)
    filter_backends = (DatatablesFilterBackend,)
    pagination_class = DatatablesPageNumberPagination
    #renderer_classes = (ProposalRenderer,)
    renderer_classes = (DatatablesRenderer,)
    queryset = Task.objects.none()
    serializer_class = TaskSerializer
    #search_fields = ['lodgement_number',]
    page_size = 10

    def get_queryset(self):
        return Task.objects.all()
        
    @action(detail=False, methods=['GET',])
    def task_datatable_list(self, request, *args, **kwargs):
        """ http://localhost:8002/api/v1/task_paginated/task_datatable_list/?format=datatables&draw=1&length=10 """
        queryset = self.get_queryset()

        #queryset = self.filter_queryset(queryset)
        self.paginator.page_size = queryset.count()
        result_page = self.paginator.paginate_queryset(queryset, request)
        serializer = TaskSerializer(
            result_page, context={'request': request}, many=True
        )
        data = serializer.data

        response = self.paginator.get_paginated_response(data)
        return response

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



