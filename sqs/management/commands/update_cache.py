from django.core.management.base import BaseCommand
from django.conf import settings

from datetime import datetime
from pytz import timezone

from sqs.components.gisquery.models import Layer
from sqs.utils.loader_utils import LayerLoader, DbLayerProvider

import logging
logger = logging.getLogger(__name__)

class Command(BaseCommand):
    """
    Load Layer util
    """

    help = 'Updates layer cache'

    def handle(self, *args, **options):

        errors = []
        updates = []
        now = datetime.now().astimezone(timezone(settings.TIME_ZONE))
        logger.info('Running command {}'.format(__name__))

        layers = Layer.active_layers.all()
        print(f'No. Layers: {layers.count()}')
        for idx, layer in enumerate(layers):
            try:
                logger.info(f'{idx}: Updating {layer.name} ...')
                layer_provider = DbLayerProvider(layer.name, layer.url)
                layer_provider.clear_cache()

                layer_info = layer_provider.layer_info(layer)
                #layer_provider.set_cache(layer_info, layer.to_gdf)
                layer_provider.set_cache(layer_info, layer.geojson)

                logger.info(f'{idx}: Layer Cache Updated {layer.name}: Date: {now}')
                updates.append(layer.name)
            except Exception as e:
                err_msg = 'Error updating cache {}'.format(layer.name)
                logger.error('{}\n{}'.format(err_msg, str(e)))
                errors.append(err_msg)

        cmd_name = __name__.split('.')[-1].replace('_', ' ').upper()
        err_str = '<strong style="color: red;">Errors: {}</strong>'.format(len(errors)) if len(errors)>0 else '<strong style="color: green;">Errors: 0</strong>'
        msg = '<p>{} completed. {}. IDs updated: {}.</p>'.format(cmd_name, err_str, updates)
        logger.info(msg)
        print(msg) # will redirect to cron_tasks.log file, by the parent script

