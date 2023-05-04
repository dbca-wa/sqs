from django.conf import settings
import traceback

from sqs.decorators import traceback_exception_handler


TEXT  = 'Text'
INT   = 'Int'
FLOAT = 'Float'

TEXT_WIDGETS = ['text', 'text_area']

DATE_FMT = '%Y-%m-%d'
DATETIME_FMT = '%Y-%m-%d %H:%M:%S'


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
                if question['layer_name'] not in layer_names:
                    layer_names.append(question['layer_name'])
        return layer_names
