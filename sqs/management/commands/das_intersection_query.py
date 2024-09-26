from django.core.management.base import BaseCommand
from django.conf import settings

import time
from datetime import datetime
import pytz

from sqs.components.gisquery.models import Layer, LayerRequestLog, Task
from sqs.utils.das_schema_utils import DisturbanceLayerQuery

import logging
logger = logging.getLogger(__name__)

class Command(BaseCommand):
    """
    Runs DAS Layer Task
        Eg. python manage.py das_intersection_query --task_id 10 '''
    """

    help = 'Runs DAS Layer Task'

    def add_arguments(self, parser):
        parser.add_argument('--task_id', type=str, help='Task ID', required=True)

    def handle(self, *args, **options):
        start_time = time.time()
        task_id = options['task_id']
        logger.info(f'Executing command \'python manage.py das_intersection_query --task_id {task_id}\'')
        try:
            task = Task.objects.get(id=task_id)
            if task.status != Task.STATUS_CREATED:
                logger.warn(f'Task status is \'{task.status.upper()}\'. Task status must be \'CREATED\'. Aborting command')
                return

            if not task.start_time:
                task.start_time = datetime.now().replace(tzinfo=pytz.utc)
            task.status = Task.STATUS_RUNNING
            task.save()
            data = task.data

            proposal     = data['proposal']
            current_ts   = proposal.get('current_ts') # only available following subsequent Prefill requests
            geojson      = data.get('geojson')
            request_type = data.get('request_type')
            system       = data.get('system')
            masterlist_questions = data.get('masterlist_questions')

            request_log = LayerRequestLog.create_log(data, request_type)

            dlq = DisturbanceLayerQuery(masterlist_questions, geojson, proposal)
            response = dlq.query()
            #response['sqs_log_url'] = request.build_absolute_uri().replace('das/spatial_query', f'logs/{request_log.id}/request_log')
            sqs_log_url = f'http://localhost:8002/api/v1/logs/{request_log.id}/request_log/'
            response['sqs_log_url'] = sqs_log_url
            response['request_type'] = request_type
            response['when'] = request_log.when.strftime("%Y-%m-%dT%H:%M:%S")
      
            request_log.response = response
            total_time = round(time.time() - start_time, 3)
            request_log.response.update({
                'metrics': dict(
                    total_query_time=total_time,
                    spatial_query=dlq.lq_helper.metrics,
                )
            })
            request_log.save()

            task.request_log_id = request_log.id
            task.status = Task.STATUS_COMPLETED
            task.time_taken = total_time
            task.end_time = datetime.now().replace(tzinfo=pytz.utc)
            task.save()
            logger.info(f'Request Log ID {request_log.id}, URL {sqs_log_url}, Time Take {total_time}')

        except Exception as e:
            if task:
                task.status = Task.STATUS_ERROR
                task.save()
            err_msg = f'Error Running {__name__}, Proposal {proposal["id"]}'
            logger.error('{}\n{}'.format(err_msg, str(e)))

