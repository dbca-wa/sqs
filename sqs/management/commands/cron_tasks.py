from django.core.management.base import BaseCommand
from django.utils import timezone
from django.conf import settings
from django.core.mail import send_mail
from pathlib import Path
import datetime

import itertools
import subprocess

import logging
logger = logging.getLogger(__name__)

LOGFILE = 'logs/cron_tasks.log'

class Command(BaseCommand):
    help = 'Run the Spatial Query System Cron tasks'

    def handle(self, *args, **options):
        stdout_redirect = ' | tee -a {}'.format(LOGFILE)
        subprocess.call('cat /dev/null > {}'.format(LOGFILE), shell=True)  # empty the log file

        logger.info('Running command {}'.format(__name__))
        subprocess.call('python manage.py update_active_layers' + stdout_redirect, shell=True) 
        subprocess.call('python manage.py update_cache' + stdout_redirect, shell=True) 

        logger.info('Command {} completed'.format(__name__))
        self.send_email()

    def send_email(self):
        log_txt = Path(LOGFILE).read_text()
        subject = '{} - Cronjob'.format(settings.SYSTEM_NAME_SHORT)
        body = ''
        to = settings.CRON_NOTIFICATION_EMAIL if isinstance(settings.CRON_NOTIFICATION_EMAIL, list) else [settings.CRON_NOTIFICATION_EMAIL]
        send_mail(subject, body, settings.EMAIL_FROM, to, fail_silently=False, html_message=log_txt)
        #send_mail('JM SQS', body, 'jawaid.mushtaq@dbca.wa.gov.au', to, fail_silently=False, html_message=log_txt)
