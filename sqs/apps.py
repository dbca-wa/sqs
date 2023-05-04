from __future__ import unicode_literals
from django.conf import settings

from django.apps import AppConfig

class SpatialQuerySystemConfig(AppConfig):
    name = 'sqs'
    verbose_name = settings.SYSTEM_NAME

    run_once = False
    def ready(self):
#        if not self.run_once:
#            from sqs.components.organisations import signals
#            from sqs.components.proposals import signals

        self.run_once = True
        pass
