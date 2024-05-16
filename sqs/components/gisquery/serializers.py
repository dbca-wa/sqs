from django.conf import settings
from django.db.models import Q

from rest_framework import serializers
from rest_framework_gis.serializers import GeoFeatureModelSerializer
#from reversion.models import Version

from sqs.components.gisquery.models import (
    Layer,
    LayerRequestLog,
    Task,
)


class DefaultLayerSerializer(serializers.ModelSerializer):
    class Meta:
        model = Layer
        geo_field = 'geojson'
        fields=(
            'id',
            'name',
            'url',
            'version',
            'active',
            'geojson_file',
        )

    def __init__(self, *args, **kwargs):
        remove_fields = kwargs.pop('remove_fields', None)
        super().__init__(*args, **kwargs)

        if remove_fields:
            # for multiple fields in a list
            for field_name in remove_fields:
                self.fields.pop(field_name)


class GeoJSONLayerSerializer(serializers.ModelSerializer):
    class Meta:
        model = Layer
        geo_field = 'geojson'
        fields=(
            'geojson',
        )


class LayerRequestLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = LayerRequestLog
        fields=(
            'id',
            'request_type',
            'system',
            'app_id',
            'when',
            'data',
            'response',
        )

    def __init__(self, *args, **kwargs):
        remove_fields = kwargs.pop('remove_fields', None)
        super().__init__(*args, **kwargs)

        if remove_fields:
            # for multiple fields in a list
            for field_name in remove_fields:
                self.fields.pop(field_name)


class TaskSerializer(serializers.ModelSerializer):
    request_log = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = Task
        fields=(
            'id',
            'app_id',
            'system',
            'requester',
            'description',
            'script',
            'parameters',
            'status',
            'priority',
            'time_taken',
            'created',
            'stdout',
            'stderr',
            'request_log',
        )

    def __init__(self, *args, **kwargs):
        remove_fields = kwargs.pop('remove_fields', None)
        super().__init__(*args, **kwargs)

        if remove_fields:
            # for multiple fields in a list
            for field_name in remove_fields:
                self.fields.pop(field_name)

    def get_time_taken(self, obj):
        return obj.time_taken()

    def get_request_log(self, obj):
        # return only the reponse component (which contains the results from the layer intersection.
        return obj.request_log.response if hasattr(obj.request_log, 'response' ) else None


