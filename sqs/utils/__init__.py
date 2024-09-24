from django.conf import settings
import traceback
import gc
import time

from sqs.decorators import traceback_exception_handler


TEXT  = 'Text'
INT   = 'Int'
FLOAT = 'Float'

TEXT_WIDGETS = ['text', 'text_area']
RADIOBUTTONS = 'radiobuttons'
CHECKBOX = 'checkbox'
MULTI_SELECT = 'multi-select'
SELECT = 'select'

DATE_FMT = '%Y-%m-%d'
DATETIME_FMT = '%Y-%m-%d %H:%M:%S'
DATETIME_T_FMT = '%Y%m%dT%H%M%S'

import logging
logger = logging.getLogger(__name__)
logger_stats = logging.getLogger('request_stats')


class HelperUtils():

    @classmethod 
    def get_type(self, string):
        if not isinstance(string, str):
            string = str(string)

        if "." in string and string.replace(".", "").isnumeric():
            return FLOAT
        elif "." not in string and string.isdigit():
            return INT
        else:
            return TEXT

    @classmethod 
    def pop_list(self, _list):
        '''
        helper to clear strings from list for layer_gdf (geoDataFrame) Exception output
        '''
        if 'id' in _list:
            _list.remove('id')

        if 'md5_rowhash' in _list:
            _list.remove('md5_rowhash')

        if 'geometry' in _list:
            _list.remove('geometry')

        return _list

    @classmethod 
    @traceback_exception_handler
    def get_layer_names(self, masterlist_questions):
        layer_names = []
        for question_group in masterlist_questions:
            for question in question_group['questions']:
                if question['layer']['layer_name'] not in layer_names:
                    layer_names.append(question['layer']['layer_name'])
        return layer_names

#    @classmethod 
#    def force_gc(self, data=None):
#        if not data:
#            gc.collect()
#        else:
#            data = [data] if not isinstance(data, list) else data
#            for df in data:
#                del df
 
    @classmethod 
    def force_gc(self, data=None):
        del data
        gc.collect()

    @classmethod 
    def log_elapsed_time(self, start_time, msg=''):
        if settings.LOG_ELAPSED_TIME:
            resp = f'Time Taken: {time.time() - start_time:.2f}s'
            logger.info(msg + ': ' + resp if msg else resp)

    @classmethod 
    def log_request(self, msg=''):
        if settings.LOG_REQUEST_STATS:
            logger_stats.info(msg)
