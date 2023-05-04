from django.contrib import admin
from django.conf import settings

from sqs.components.api.models import API

@admin.register(API)
class APIAdmin(admin.ModelAdmin):
    list_display = ('id','system_name','system_id','active')
    exclude = ['api_key']
