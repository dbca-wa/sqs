from json_checker import Checker, OptionalKey

import logging
logger = logging.getLogger(__name__)


# t = Task.objects.filter(request_log__data__isnull=False).last()
# t.data.keys()
# is_valid_schema(t.data, EXPECTED_SCHEMA)
EXPECTED_SCHEMA = {
    'proposal': {
        'id': int,
        'current_ts': str,
        'schema': list,
        'data': list,
    },
    OptionalKey('request_type'): str,
    'system': str,
    'masterlist_questions': list,
    'geojson': dict,
}

# t = Task.objects.filter(request_log__data__isnull=False).last()
# t.data['masterlist_questions'][0].keys()
# is_valid_schema(t.data['masterlist_questions'][0], EXPECTED_MASTERLIST_QUESTION_GROUP)
EXPECTED_MASTERLIST_QUESTION_GROUP = {
    'questions': list,
    'question_group': str,
}

# t = Task.objects.filter(request_log__data__isnull=False).last()
# t.data['masterlist_questions'][0]['questions'][0].keys()
# is_valid_schema(t.data['masterlist_questions'][0]['questions'][0], EXPECTED_MASTERLIST_QUESTION)
EXPECTED_MASTERLIST_QUESTION = {
    'id': int,
    'how': str,
    'group': {
        'id': int, 
        'name': str, 
        'can_user_edit': bool,
    },
    'layer': {
        'id': int,
        'layer_url': str,
        'layer_name': str,
    },
    'value':                                    object,
    'answer':                                   object,
    'buffer':                                   int,
    'expiry':                                   object,
    'operator':                                 str,
    'question':                                 str,
    'answer_mlq':                               object,
    'column_name':                              str,
    'prefix_info':                              object,
    'assessor_info':                            object,
    'modified_date':                            object,
    'prefix_answer':                            object,
    'assessor_items':                           object,
    'proponent_items':                          object,
    'masterlist_question':                      list,
    'visible_to_proponent':                     bool,
    OptionalKey('regions'):                     object,
    OptionalKey('no_polygons_assessor'):        object,
    OptionalKey('no_polygons_proponent'):       object,
}

# t = Task.objects.filter(request_log__data__isnull=False).last()
# is_valid_schema(t.data)
EXPECTED_SCHEMA_FULL = {
    'proposal': {
        'id': int,
        'schema': list,
        OptionalKey('current_ts'): object,
        OptionalKey('data'): object,
    },
    OptionalKey('request_type'): str,
    'requester': str,
    'system': str,
    'geojson': dict,
    'masterlist_questions':[{
        'question_group': str,
        'questions': [
            {
                'id': int,
                'how': str,
                'group': {
                    'id': int, 
                    'name': str, 
                    'can_user_edit': bool,
                },
                'layer': {
                    'id': int,
                    'layer_url': str,
                    'layer_name': str,
                },
                'value':                                    object,
                'answer':                                   object,
                'buffer':                                   int,
                'expiry':                                   object,
                'operator':                                 str,
                'question':                                 str,
                'answer_mlq':                               object,
                'column_name':                              str,
                'prefix_info':                              object,
                'assessor_info':                            object,
                'modified_date':                            object,
                'prefix_answer':                            object,
                'assessor_items':                           object,
                'proponent_items':                          object,
                'masterlist_question':                      list,
                'visible_to_proponent':                     bool,
                'show_add_info_section_prop':               bool,
                OptionalKey('regions'):                     object,
                OptionalKey('no_polygons_assessor'):        object,
                OptionalKey('no_polygons_proponent'):       object,
            }
        ], 
    }],
}


def check_schema(data, expected_schema):
    '''
        from sqs.components.gisquery.utils.schema import check_schema, EXPECTED_MASTERLIST_QUESTION, EXPECTED_SCHEMA, EXPECTED_SCHEMA

        t = Task.objects.filter(request_log__data__isnull=False).last()
        check_schema(t.data, EXPECTED_SCHEMA)
        check_schema(t.data['masterlist_questions'][0], EXPECTED_MASTERLIST_QUESTION_GROUP)
        check_schema(t.data['masterlist_questions'][0]['questions'][0], EXPECTED_MASTERLIST_QUESTION)
    '''
    return Checker(expected_schema).validate(data) == data

def _is_valid_schema(data):
    '''
        from sqs.components.gisquery.utils.schema import is_valid_schema
        t = Task.objects.filter(request_log__data__isnull=False).last()
        _is_valid_schema(t.data)
    '''
    try:
        is_valid1 = check_schema(data, EXPECTED_SCHEMA)
        is_valid2 = check_schema(data['masterlist_questions'][0], EXPECTED_MASTERLIST_QUESTION_GROUP)
        is_valid3 = check_schema(data['masterlist_questions'][0]['questions'][0], EXPECTED_MASTERLIST_QUESTION)

        if is_valid1 and is_valid2 and is_valid3:
            return True

    except Exception as e:
        logger.error(str(e))

    logger.error(f'{is_valid1} - {is_valid2} - {is_valid3}')
    return False

def is_valid_schema(data):
    '''
        from sqs.components.gisquery.utils.schema import is_valid_schema
        t = Task.objects.filter(request_log__data__isnull=False).last()
        is_valid_schema(t.data)
    '''
    is_valid = False
    try:
        is_valid = check_schema(data, EXPECTED_SCHEMA_FULL)

        if is_valid:
            return True

    except Exception as e:
        logger.error(str(e))

    logger.error(f'{is_valid}')
    return False
