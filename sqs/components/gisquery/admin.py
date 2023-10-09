from django.contrib import admin
from sqs.components.gisquery.models import Layer, LayerRequestLog

@admin.register(Layer)
class LayerAdmin(admin.ModelAdmin):
    list_display = ["name", "url", "version", "active"]
    search_fields = ['name__icontains']
    readonly_fields = ('geojson',)

#    def get_fields(self, request, obj=None):
#        fields = super(LayerAdmin, self).get_fields(request, obj)
#        fields_list = list(fields)
#        if obj:
#            fields_list.remove('geojson')
#        fields_tuple = tuple(fields_list)
#        return fields_tuple

@admin.register(LayerRequestLog)
class LayerRequestLogAdmin(admin.ModelAdmin):
    list_display = ["system", "app_id", "when"]
    search_fields = ['system', 'app_id', 'when']



