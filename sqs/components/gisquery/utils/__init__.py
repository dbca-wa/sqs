from django.conf import settings
from django.core.cache import cache
from django.http import JsonResponse
from rest_framework import status

import traceback

import logging
logger = logging.getLogger(__name__)


def clear_cache(key):
    if cache_key:
        cache.delete(cache_key)
        return True
    return False

def set_das_cache(data):
    ''' check request cache to prevent repeated requests while previous request is still running
    '''

    def get_question_ids():
        ''' For request_type=='PARTIAL'/'SINGLE', need to append masterlist question ids to the cack_key,
            for a unique key
        '''
        try:
            ids = '_'.join([str(q['id']) for q in masterlist_questions[0]['questions']])
        except Exception as qe:
            ids = None
        return ids

    cache_key = None
    try:
        proposal = data['proposal']
        masterlist_questions = data['masterlist_questions']
        request_type = data['request_type']
        system = data['system']
        current_ts = proposal.get('current_ts')

        if request_type == 'FULL':
            cache_key = f'{system}_{request_type}_{proposal["id"]}'
            cache_timeout = settings.REQUEST_CACHE_TIMEOUT
        else:
            ids = get_question_ids()
            cache_key = f'{system}_{request_type}_{proposal["id"]}_{ids}'
            cache_timeout = settings.REQUEST_PARTIAL_CACHE_TIMEOUT
            
        cache_data = cache.get(cache_key)
        if cache_data:
            msg = f'Request is already running ({cache_key}: {cache_data.get("timestamp")}) ...'
            return  JsonResponse(status=status.HTTP_400_BAD_REQUEST, data={'errors': msg})
        cache_data = dict(system=system, request_type=request_type, app_id=proposal['id'], timestamp=current_ts)
        cache.set(cache_key, cache_data, cache_timeout)

    except Exception as e:
        if cache_key:
            cache.delete(cache_key)
        logger.error(traceback.print_exc())

    logger.info(f'CACHE_KEY: {cache_key}')
    return cache_key
