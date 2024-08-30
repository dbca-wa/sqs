from django.conf import settings
#from django.contrib import admin
from sqs.admin import admin
#from django.conf.urls import url, include
from django.conf.urls import include
from django.urls import path, re_path
from django.contrib.auth.views import LogoutView, LoginView
from django.views.decorators.csrf import csrf_exempt

from django.contrib.auth import logout, login # DEV ONLY
from django.views.generic import TemplateView

from django.conf.urls.static import static
from rest_framework import routers
from rest_framework_swagger.views import get_swagger_view
from sqs import views
from sqs.components.gisquery import api as gisquery_api
from sqs.components.gisquery import views as gisquery_views
#from ledger_api_client.urls import urlpatterns as ledger_patterns

schema_view = get_swagger_view(title='SQS API')

# API patterns
router = routers.DefaultRouter()
router.register(r'layers', gisquery_api.DefaultLayerViewSet, basename='layers')
router.register(r'logs', gisquery_api.LayerRequestLogViewSet, basename='logs')
router.register(r'point_query', gisquery_api.PointQueryViewSet, basename='point_query')
router.register(r'tasks', gisquery_api.TaskViewSet, basename='tasks')
#router.register(r'das', gisquery_api.DisturbanceLayerViewSet, basename='das')
router.register(r'task_paginated', gisquery_api.TaskPaginatedViewSet, basename='task_paginated')

api_patterns = [
    re_path(r'^api/v1/',include(router.urls)),
]

# URL Patterns
urlpatterns = [
    re_path(r'admin/', admin.site.urls),
    re_path(r'^logout/$', LogoutView.as_view(), {'next_page': '/'}, name='logout'),
    re_path(r'', include(api_patterns)),
    re_path(r'^$', TemplateView.as_view(template_name='sqs/base2.html'), name='home'),

    re_path(r'api/v1/das/task_queue', csrf_exempt(gisquery_views.DisturbanceLayerQueueView.as_view()), name='das_task_queue'),
    re_path(r'api/v1/das/spatial_query', csrf_exempt(gisquery_views.DisturbanceLayerView.as_view()), name='das_spatial_query'),
    re_path(r'api/v1/add_layer', csrf_exempt(gisquery_views.DefaultLayerProviderView.as_view()), name='add_layer'),
    #url(r'api/v1/point_query', csrf_exempt(gisquery_views.PointQueryLayerView.as_view()), name='point_query'),
    #url(r'api/v1/check_layer', csrf_exempt(gisquery_views.CheckLayerView.as_view()), name='check_layer'),

    #url(r'api/v1/das/(?P<apikey>[\w\-]+)', csrf_exempt(gisquery_views.DisturbanceLayerView.as_view()), name='das'),
#    url(r'api/v1/view_test', csrf_exempt(gisquery_views.TestView.as_view()), name='view_test'),
#    url(r'api/v1/das2/(?P<apikey>[\w\-]+)', csrf_exempt(gisquery_api.DisturbanceLayerAPIView.as_view()), name='das2'),
#    url(r'api/v1/das3/(?P<apikey>[\w\-]+)', gisquery_api.DisturbanceLayerAPIView2.as_view(), name='das3'),
#    url(r'api/v1/das4', csrf_exempt(gisquery_api.DisturbanceLayerAPIView3.as_view()), name='das4'),
#    url(r'api/v1/das5', gisquery_api.DisturbanceLayerAPIView3.as_view(), name='das5'),

    re_path(r'schema/', schema_view),
]

#urlpatterns += [re_path('silk/', include('silk.urls', namespace='silk'))]

#if settings.SHOW_DEBUG_TOOLBAR:
#    import debug_toolbar
#    urlpatterns = [
#        url('__debug__/', include(debug_toolbar.urls)),
#    ] + urlpatterns
