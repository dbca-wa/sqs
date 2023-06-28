from django.conf import settings
from django.db.models import Q

from rest_framework import serializers
from rest_framework_gis.serializers import GeoFeatureModelSerializer
#from reversion.models import Version

from sqs.components.gisquery.models import (
    Layer,
    LayerRequestLog
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
            #'geojson',
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

#class DisturbanceLayerSerializer(serializers.ModelSerializer):
#    class Meta:
#        model = Layer
#        geo_field = 'geojson'
#        fields=(
#            'id',
#            'name',
#            'url',
#            'version',
#            'geojson',
#        )


