from django.conf import settings
from django.db.models import Q

from rest_framework import serializers
from rest_framework_gis.serializers import GeoFeatureModelSerializer
#from reversion.models import Version

from sqs.components.gisquery.models import (
    Layer,
)


class GeoTestSerializer(serializers.ModelSerializer):

    class Meta:
        model = Layer
        #fields = '__all__'
        fields=(
            'id',
            'name',
            'type',
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
            'geojson',
        )


class DisturbanceLayerSerializer(serializers.ModelSerializer):
    class Meta:
        model = Layer
        geo_field = 'geojson'
        fields=(
            'id',
            'name',
            'url',
            'version',
            'geojson',
        )


