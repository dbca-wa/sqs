from django.contrib import admin
from sqs.components.gisquery.models import Layer, LayerRequestLog, Task

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


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ['description', 'script', 'status', 'priority', 'position']
    search_fields = ['description', 'script', 'status', 'priority']
    readonly_fields = ('start_time', 'end_time', 'time_taken', 'stdout', 'stderr', 'description', 'parameters', 'created_date', 'data')

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.filter().order_by('-created')

    def time_taken(self, obj):
        return obj.time_taken()

    def position(self, obj):
        return obj.position

