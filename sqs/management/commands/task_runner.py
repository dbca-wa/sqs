from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

import subprocess
from datetime import datetime
from django.utils import timezone

import os
import sys
import json
import time
import pytz
from sqs.components.gisquery.models import Layer, LayerRequestLog, Task


import logging
logger = logging.getLogger(__name__)

DATE_FMT = os.environ.get('DATE_FMT')


class Command(BaseCommand):
    help = 'Runs the queued scripts (in TaskQueue) via the distributor'

    def handle(self, *args, **options):
        TaskRunner().run()


class TaskRunner(object):

    def run(self):

        task_runner_cmd = f'manage.py {__name__.split(".")[-1]}' # 'manage.py task_runner'
        if self.num_running_processes(task_runner_cmd) > 1:
            logger.warn(f'TaskRunner already running. Aborting script {__name__} ...')
            return

        while task := self.next_task_from_queue():
            try:

                cmd = self.get_cmd(task)
                if self.num_running_processes(task.script) > 3:
                    logger.warn(f'Too many processes spawned. Aborting command \'{cmd}\' ...')
                    continue

                #start_time = datetime.now().replace(tzinfo=pytz.utc)
                start_time = timezone.localtime()
                logger.info('_' * 120)
                logger.info(f'Executing command \'{cmd}\'')
                result = subprocess.run(cmd, capture_output=True, text=True, check=True, shell=True)
                time_taken = self.update_task_metrics(task.id, result, start_time)
                logger.info(f'Completed command \'{cmd}\'. ({task.system}_{task.app_id}) - Time Taken {time_taken} mins')

            except Exception as e:
                logger.error(f'Failed to run task\n{e}')
                if task:
                    task.error = str(e)
                    task.status = Task.STATUS_FAILED
                    task.save()

            task = None

    def get_cmd(self, task):
        ''' Eg. python manage.py das_intersection_query --task_id 10 '''
        script_args = task.parameters if task.parameters else ''
        return f'{task.script} {script_args} --task_id {task.id}'

    def next_task_from_queue(self):
        task = Task.queued_jobs.filter().order_by('priority', 'created').first()
        if task:
            if task.retries < settings.MAX_RETRIES:
                task.retries = task.retries + 1
                task.save()
            else:
                task.status = Task.STATUS_MAX_RETRIES_REACHED
                task.save()
        return task

#    def increment_retries(self, task):
#        if task and task.retries < settings.MAX_RETRIES:
#            task.retries = task.reries + 1
#            task.save()
#        else:
#            task.status = Task.STATUS_MAX_RETRIES_REACHED
#            task.save()
#        return task


    def num_running_processes(self, cmd):
        ''' Returns the number of scripts/processes running '''
        num_cmds = 0
        try:
            pgrep = f'ps aux | grep -v grep | grep -v run-one | grep -v color | grep "{cmd}"  | wc -l'
            num_cmds = int(subprocess.check_output(pgrep, shell=True).strip())

        except Exception as e:
            raise Exception(f'Error getting number of running processes/commands {__name__}\n{e}')

        return num_cmds

    def update_task_metrics(self, task_id, result, start_time):
        try:
            task = Task.objects.get(id=task_id)

            #task.stdout = json.dumps(result.__dict__)
            if not task.start_time:
                task.start_time = start_time
            if not task.end_time:
                #task.end_time = datetime.now().replace(tzinfo=pytz.utc)
                task.end_time = timezone.localtime()
            task.stdout = result.stdout
            task.stderr = result.stderr
            task.status = Task.STATUS_COMPLETED
        except Exception as e:
            logger.error(f'Error updating Task metrics {task.description}\n{e}')
            #task.stdout = result.__str__()
            task.stdout = result.stdout + '\n' + result.stderr 
            task.stderr = str(e)
            task.status = Task.STATUS_ERROR

        #task.end_date = datetime.now()
        task.end_time = timezone.localtime()
        task.save()
        return task.time_taken()


