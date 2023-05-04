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

    help = 'Updates the actice layers - get the active layers from GeoServer and if changed update SQS'

    def handle(self, *args, **options):
        #import ipdb; ipdb.set_trace()
        errors = []
        updates = []
        now = datetime.now().astimezone(timezone(settings.TIME_ZONE))
        logger.info('Running command {}'.format(__name__))

        for layer in Layer.active_layers.all():
            try:
                # get layer from GeoServer and update SQS layer if changed.
                new_layer = LayerLoader(name=layer.name, url=layer.url).load_layer()

                if layer.modified_date != new_layer.modified_date:
                    layer_provider = DbLayerProvider(new_layer.name, new_layer.url)
                    layer_provider.clear_cache()

                    layer_info = layer_provider._layer_info(layer)
                    layer_provider.set_cache(layer_info, layer.to_gdf)

                    url_text = f'NEW URL: {new_layer.url}' if layer.url != new_layer.url else f'URL: {layer.url}'
                    logger.info(f'Layer Updated: {new_layer.name}, Modified Date: {new_layer.modified_date}, Version: {new_layer.version}\n{url_text}\n{"_"*125}')
                    updates.append([new_layer.name, new_layer.version])

            except Exception as e:
                err_msg = 'Error updating layer {}'.format(layer.name)
                logger.error('{}\n{}'.format(err_msg, str(e)))
                errors.append(err_msg)

        cmd_name = __name__.split('.')[-1].replace('_', ' ').upper()
        err_str = '<strong style="color: red;">Errors: {}</strong>'.format(len(errors)) if len(errors)>0 else '<strong style="color: green;">Errors: 0</strong>'
        msg = '<p>{} completed. {}. IDs updated: {}.</p>'.format(cmd_name, err_str, updates)
        logger.info(msg)
        print(msg) # will redirect to cron_tasks.log file, by the parent script

