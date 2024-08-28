from django.core.management.base import BaseCommand
from django.conf import settings

from datetime import datetime
from pytz import timezone
from dateutil import parser

from sqs.components.gisquery.models import Layer
from sqs.utils.loader_utils import LayerLoader, DbLayerProvider
from sqs.utils.loader_utils import RecentLayerProvider

import logging
logger = logging.getLogger(__name__)

'''
TODO - update script to check for modified date, and update only those found with a newer modified_date

import requests
r=requests.get('https://kaartdijin-boodja.dbca.wa.gov.au/api/catalogue/entries/recent/?days_ago=30', auth=(settings.LEDGER_USER,settings.LEDGER_PASS))
r.json()
'''
class Command(BaseCommand):
    """
    Load layer utility

    # will retrieve a list of recently updated layers from KB, cross-check if they exist in SQS - if they exist then update in SQS
    ./manage.py update_layers --days_ago 7

    # update user provided layer names, only if recently updated (--name must be last paramenter)
    ./manage.py update_layers --name CPT_DBCA_REGIONS CPT_THREATENED_FAUNA
    ./manage.py update_layers --days_ago 7 --name CPT_DBCA_REGIONS CPT_THREATENED_FAUNA 

    # force update of user provided layer names - ignore recently updated check (--name must be last paramenter)
    ./manage.py update_layers --force --name CPT_DBCA_REGIONS CPT_THREATENED_FAUNA

    To get a list of all existing layer_names 
    ./manage.py update_layers --list
    """

    help = 'Updates the active layers - get the active layers from GeoServer and if changed update SQS'

    def add_arguments(self, parser):
        parser.add_argument('--days_ago', type=int, help='Layers changed in KB with last <int: days_ago>', default=7)
        parser.add_argument('--name', type=str, help='Update layer by name from KB', nargs='*') # optional
        parser.add_argument('--force', action='store_true', help='Update layer by name from KB, ignore recently updated flag')
        parser.add_argument('--list', action='store_true', help='List current stored layer in DB')

    def handle(self, *args, **options):
        days_ago = options['days_ago']
        layers = options['name'] if options['name'] else []
        force = options['force']
        list_layers_in_db = options['list']

        errors = []
        updates = []
        now = datetime.now().astimezone(timezone(settings.TIME_ZONE))
        logger.info('Running command {}'.format(__name__))

        if list_layers_in_db: 
            return ' '.join(list(Layer.objects.all().values_list('name', flat=True)))

        if not force: 
            # get recent layers changed in KB
            #layers_to_update = RecentLayerProvider(days_ago=days_ago).get_layers_to_update()
            layers = RecentLayerProvider(days_ago=days_ago).get_layers_to_update()

            # combine with user provided layers and remove duplicates
            #layers = list(set(layers + layers_to_update))

        # update layers in SQS
        for layer_name in layers:
            try:
                # get layer from KB and update SQS layer
                new_layer = LayerLoader(name=layer_name).load_layer()

                logger.info(f'Layer Updated: {new_layer.name}, Modified Date: {new_layer.modified_date}, Version: {new_layer.version}\n{"_"*125}')
                updates.append([new_layer.name, new_layer.version])

            except Exception as e:
                err_msg = 'Error updating layer {}'.format(layer_name)
                logger.error('{}\n{}'.format(err_msg, str(e)))
                errors.append(err_msg)

        cmd_name = __name__.split('.')[-1].replace('_', ' ').upper()
        err_str = '<strong style="color: red;">Errors: {}</strong>'.format(len(errors)) if len(errors)>0 else '<strong style="color: green;">Errors: 0</strong>'
        msg = '<p>{} completed. {}. IDs updated: {}.</p>'.format(cmd_name, err_str, updates)
        logger.info(msg)
        print(msg) # will redirect to cron_tasks.log file, by the parent script

