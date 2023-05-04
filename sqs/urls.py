from django.conf import settings
#from django.contrib import admin
from sqs.admin import admin
from django.conf.urls import url, include
from django.urls import path
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
#router.register(r'layers/(?P<apikey>[\w\-]+)', gisquery_api.DefaultLayerViewSet, basename='layers')
#router.register(r'das/(?P<apikey>[\w\-]+)', gisquery_api.DisturbanceLayerViewSet, basename='das')
router.register(r'layers', gisquery_api.DefaultLayerViewSet, basename='layers')
router.register(r'das', gisquery_api.DisturbanceLayerViewSet, basename='das')

api_patterns = [
    url(r'^api/v1/',include(router.urls)),
]

# URL Patterns
urlpatterns = [
    path(r'admin/', admin.site.urls),
    url(r'^logout/$', LogoutView.as_view(), {'next_page': '/'}, name='logout'),
    url(r'', include(api_patterns)),
    url(r'^$', TemplateView.as_view(template_name='sqs/base2.html'), name='home'),

    #url(r'api/v1/das/(?P<apikey>[\w\-]+)', csrf_exempt(gisquery_views.DisturbanceLayerView.as_view()), name='das'),
#    url(r'api/v1/das', csrf_exempt(gisquery_views.DisturbanceLayerView.as_view()), name='das'),
#    url(r'api/v1/view_test', csrf_exempt(gisquery_views.TestView.as_view()), name='view_test'),
#    url(r'api/v1/das2/(?P<apikey>[\w\-]+)', csrf_exempt(gisquery_api.DisturbanceLayerAPIView.as_view()), name='das2'),
#    url(r'api/v1/das3/(?P<apikey>[\w\-]+)', gisquery_api.DisturbanceLayerAPIView2.as_view(), name='das3'),
#    url(r'api/v1/das4', csrf_exempt(gisquery_api.DisturbanceLayerAPIView3.as_view()), name='das4'),
#    url(r'api/v1/das5', gisquery_api.DisturbanceLayerAPIView3.as_view(), name='das5'),

    url(r'schema/', schema_view),
]

#if settings.SHOW_DEBUG_TOOLBAR:
#    import debug_toolbar
#    urlpatterns = [
#        url('__debug__/', include(debug_toolbar.urls)),
#    ] + urlpatterns
