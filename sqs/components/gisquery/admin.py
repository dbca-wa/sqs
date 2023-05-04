from django.contrib import admin
#from django.contrib.gis import admin
#from sqs.components.main import models
#from sqs.components.gisquery import models
from sqs.components.gisquery.models import Layer, LayerRequestLog

#from django.contrib.admin import AdminSite

@admin.register(Layer)
class LayerAdmin(admin.ModelAdmin):
    #fields = ["system", "app_id", "when"]
    list_display = ["name", "url", "version", "active"]


@admin.register(LayerRequestLog)
class LayerRequestLogAdmin(admin.ModelAdmin):
    list_display = ["system", "app_id", "when"]



