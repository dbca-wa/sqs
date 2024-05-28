from django.core.management.base import BaseCommand
from django.conf import settings
import subprocess
import os
import json
from pathlib import Path
from sqs.utils.loader_utils import LayerLoader, DbLayerProvider


import logging
logger = logging.getLogger(__name__)

class Command(BaseCommand):
    """
    Load Layer util

    Example:
        Can be created/update individualy Or bulk:

        1. ./manage.py layer_loader --url "https://kaartdijin-boodja.dbca.wa.gov.au/api/catalogue/entries/CPT_DBCA_REGIONS/layer/" --name "CPT_DBCA_REGIONS"

        OR

        2. Bulk Create/Update of layers from JSON file, eg. extracted via API call to DAS from browser (internal credentials means user is already authenticated) 
           - http://localhost:8003/api/das_map_layers/

            [
                {
                    "layer_name": "CPT_LOCAL_GOVT_AREAS",
                    "layer_url": "https://kaartdijin-boodja.dbca.wa.gov.au/api/catalogue/entries/CPT_LOCAL_GOVT_AREAS/layer/"
                },
                    "layer_name": "CPT_DIEBACK_VULNERABLE_ZONE",
                    "layer_url": "https://kaartdijin-boodja.dbca.wa.gov.au/api/catalogue/entries/CPT_DIEBACK_VULNERABLE_ZONE/layer/"
                },
                ...
            ]

            ./manage.py layer_loader --file /tmp/das_map_layers.json
    """

    help = 'Loads Layer from Geoserver: ./manage.py layer_loader --url "https://kaartdijin-boodja.dbca.wa.gov.au/api/catalogue/entries/CPT_DBCA_REGIONS/layer/" --name "CPT_DBCA_REGIONS"'

    def add_arguments(self, parser):
        parser.add_argument('--name', nargs='?', type=str, help='Geoserver layer name eg. "CPT_DBCA_REGIONS"', required=False)
        parser.add_argument('--url', nargs='?', type=str, help='Geoserver URL, eg. https://kaartdijin-boodja.dbca.wa.gov.au/api/catalogue/entries/CPT_DBCA_REGIONS/layer/', required=False)
        parser.add_argument('--file', nargs='?', type=str, help='file containing csv list of "layer_name, layer_url" to create/update', required=False)


    def handle(self, *args, **options):
        url = options['url']
        name = options['name']
        _file = options['file']
        logger.info('Running command {}'.format(__name__))

        if _file:
            if os.path.isfile(_file):
                data = open(_file).read()
                jsonData = json.loads(data)

                for layer in jsonData:
                    if layer['layer_name'] not in ['tec_sites_buffered', 'CPT_PHYSIOGNOMIC_VEG', 'Mining_Tenements_DMIRS_003']:
                        try:
                            logger.info(f'Layer: {layer["layer_name"]}')
                            layer_info, layer_gdf = DbLayerProvider(layer['layer_name'], layer['layer_url']).get_layer_from_geoserver()
                        except Exception as e:
                            logger.error(f'{e}')

        elif url and name:
            layer_info, layer_gdf = DbLayerProvider(name, url).get_layer_from_geoserver()
        else:
            logger.info(f'No args specified')
            

