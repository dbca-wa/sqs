from django.core.management.base import BaseCommand
from django.conf import settings
import subprocess
import os
#from sqs.utils.loader_utils import LayerLoader #, has_layer_changed
#from sqs.components.gisquery.models import Layer
from sqs.utils.loader_utils import LayerLoader, DbLayerProvider


import logging
logger = logging.getLogger(__name__)

class Command(BaseCommand):
    """
    Load Layer util
    """

    help = 'Loads Layer from Geoserver: ./manage.py layer_loader --url "https://kaartdijin-boodja.dbca.wa.gov.au/api/catalogue/entries/CPT_DBCA_REGIONS/layer/" --name "CPT_DBCA_REGIONS"'

    def add_arguments(self, parser):
        parser.add_argument('--name', nargs='?', type=str, help='Geoserver layer name eg. "CPT_DBCA_REGIONS"')
        parser.add_argument('--url', nargs='?', type=str, help='Geoserver URL, eg. https://kaartdijin-boodja.dbca.wa.gov.au/api/catalogue/entries/CPT_DBCA_REGIONS/layer/')

    def handle(self, *args, **options):
        url = options['url']
        name = options['name']
        logger.info('Running command {}'.format(__name__))

        layer_info, layer_gdf = DbLayerProvider(name, url).get_layer_from_geoserver()

