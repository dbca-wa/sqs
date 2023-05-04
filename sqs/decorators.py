from rest_framework.response import Response
from django.db import connection, reset_queries
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.handlers.wsgi import WSGIRequest
from django.http import HttpRequest
from rest_framework import serializers
from rest_framework.request import Request

import time
import traceback
import functools

from sqs.components.api import models as api_models
from sqs.components.api import utils as api_utils

import logging
logger = logging.getLogger()


def ip_check_required(func):
    ''' IP Check Required
    '''
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        request = args[0].request
        if api_utils.api_allow_ip(api_utils.get_client_ip(request)) is True or not settings.CHECK_IP:
            try:
                return func(*args, **kwargs)

            except Exception as e:
                logger.error(traceback.print_exc())
                return Response({'status': 501, 'message': 'Error', 'data':{'message': str(e)}})
        else:
            return Response({'status': 403, 'message': 'Access Forbidden'})

    return wrapper

def apikey_required(func):
    ''' IP Check and API TOKEN Required
    '''
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        request = args[0].request
        apikey = kwargs['apikey']
        if api_models.API.objects.filter(api_key=apikey,active=1).count() or not settings.CHECK_APIURL_TOKEN:
            if api_utils.api_allow(api_utils.get_client_ip(request),apikey) is True or not settings.CHECK_IP:
                try:
                    return func(*args, **kwargs)

                except Exception as e:
                    logger.error(traceback.print_exc())
                    return Response({'status': 501, 'message': 'Error', 'data':{'message': str(e)}})
            else:
                return Response({'status': 403, 'message': 'Access Forbidden'})
        else:
            return Response({'status': 404, 'message': 'API Key Not Found'})

    return wrapper

def basic_exception_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        #import ipdb; ipdb.set_trace()
        try:
            return func(*args, **kwargs)

        except serializers.ValidationError:
            print(traceback.print_exc())
            raise
        except ValidationError as e:
            if hasattr(e, 'error_dict'):
                raise serializers.ValidationError(repr(e.error_dict))
            else:
                if hasattr(e, 'message'):
                    raise serializers.ValidationError(e.message)
                else:
                    raise
        except Exception as e:
            print(traceback.print_exc())
            raise serializers.ValidationError(str(e))
    return wrapper


def traceback_exception_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            traceback.print_exc()
    return wrapper

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' % (method.__name__, (te - ts) * 1000))
            #logger.error('%r  %2.2f ms' % (method.__name__, (te - ts) * 1000))
        return result
    return timed

def query_debugger(func):
    @functools.wraps(func)
    def inner_func(*args, **kwargs):
        reset_queries()
        start_queries = len(connection.queries)
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        end_queries = len(connection.queries)
        print(f"Function : {func.__name__}")
        print(f"Number of Queries : {end_queries - start_queries}")
        print(f"Finished in : {(end - start):.2f}s")
        function_name = 'Function : {}'.format(func.__name__)
        number_of_queries = 'Number of Queries : {}'.format(end_queries - start_queries)
        time_taken = 'Finished in : {0:.2f}s'.format((end - start))
        logger.error(function_name)
        logger.error(number_of_queries)
        logger.error(time_taken)
