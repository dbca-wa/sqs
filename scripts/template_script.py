import os
import sys
import django
proj_path='/var/www/sqs'
sys.path.append(proj_path)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sqs.settings")
django.setup()


from sqs.components.proposals.models import Proposal

p=Proposal.objects.last()

print(p.__dict__)

