from json_checker import Checker, OptionalKey

import logging
logger = logging.getLogger(__name__)


## t = Task.objects.filter(request_log__data__isnull=False).last()
## is_valid_schema(t.data)
#_EXPECTED_SCHEMA_FULL = {
#    'proposal': {
#        'id': int,
#        'data': object,
#        'schema': list,
#        OptionalKey('current_ts'): object,
#        #OptionalKey('data'): object,
#    },
#    #OptionalKey('request_type'): str,
#    'request_type': str,
#    'requester': str,
#    'system': str,
#    'geojson': object,
#    'masterlist_questions':[{
#        'question_group': str,
#        'questions': [
#            {
#                'id': int,
#                'modified_date': object,
#                'masterlist_question': {
#                    'id': int,
#                    'question': str,
#                    'answer_type': str,
#                },
#                'answer_mlq': object,
#                'group': {
#                    'id': int, 
#                    'name': str, 
#                    'can_user_edit': bool,
#                },
#                'other_data': object,
#                #'layers': list,
#                'layers': [{
#                    'id': int,
#                    'how': str,
#                    'layer': {
#                        'id': int,
#                        'layer_url': str,
#                        'layer_name': str,
#                        'display_name': str,
#                    },
#                    'value': object,
#                    'answer': object,
#                    'buffer': int,
#                    'expiry': object,
#                    'operator': str,
#                    'column_name': str,
#                    'prefix_info': str,
#                    'assessor_info': str,
#                    'modified_date': object,
#                    'prefix_answer': str,
#                    'assessor_items': object,
#                    'proponent_items': object,
#                    'visible_to_proponent': bool,
#                    'spatial_query_question_id': int,
#                    OptionalKey('regions'): object,
#                    OptionalKey('no_polygons_assessor'): object,
#                    OptionalKey('no_polygons_proponent'): object,
#                }],
#            }
#        ], 
#    }],
#}

PROPOSAL_SCHEMA = {
    'id': int,
    'data': object,
    'schema': list,
    OptionalKey('current_ts'): object,
}

MASTERLIST_QUESTION_SCHEMA = {
    'id': int,
    'question': str,
    'answer_type': str,
}

GROUP_SCHEMA = {
    'id': int, 
    'name': str, 
    'can_user_edit': bool,
}

MAP_LAYER_SCHEMA = {
    'id': int,
    'layer_url': str,
    'layer_name': str,
    'display_name': str,
}

LAYERS_SCHEMA = {
    'id': int,
    'how': str,
    #'layer': MAP_LAYER_SCHEMA,
    'layer': object,
    'value': object,
    'answer': object,
    'buffer': int,
    'expiry': object,
    'operator': str,
    'column_name': str,
    'prefix_info': str,
    'assessor_info': str,
    'modified_date': object,
    'prefix_answer': str,
    'assessor_items': object,
    'proponent_items': object,
    'visible_to_proponent': bool,
    'spatial_query_question_id': int,
    OptionalKey('regions'): object,
    OptionalKey('no_polygons_assessor'): object,
    OptionalKey('no_polygons_proponent'): object,
}

# t = Task.objects.filter(request_log__data__isnull=False).last()
# is_valid_schema(t.data)
EXPECTED_SCHEMA_FULL = {
    'proposal': PROPOSAL_SCHEMA,
    'request_type': str,
    'requester': str,
    'system': str,
    'geojson': object,
    'masterlist_questions':[{
        'question_group': str,
        'questions': [
            {
                'id': int,
                'modified_date': object,
                'masterlist_question': MASTERLIST_QUESTION_SCHEMA,
                'answer_mlq': object,
                'group': GROUP_SCHEMA,
                'other_data': object,
                'layers': object,
                #'layers': [LAYERS_SCHEMA],
            },
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
