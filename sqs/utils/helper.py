from collections import OrderedDict
import json
import geopandas as gpd

from sqs.utils import (
    HelperUtils,
    TEXT,
    INT,
    FLOAT,
    TEXT_WIDGETS
)

import logging
logger = logging.getLogger(__name__)


GREATER_THAN = 'GreaterThan'
LESS_THAN    = 'LessThan'
EQUALS       = 'Equals'


class DefaultOperator():
    '''
        cddp_question => overlay result from gpd.overlay (intersection, difference etc) --> dict
        overlay_gdf   => overlay gdf from gpd.overlay (intersection, difference etc)    --> GeoDataFrame
    '''
    def __init__(self, cddp_question, overlay_gdf, widget_type):
        self.cddp_question = cddp_question
        self.overlay_gdf = overlay_gdf
        self.widget_type = widget_type
        self.operator_result = []

    def cast_list(self, value, overlay_result):
        ''' cast all array items to value type, discarding thos that cannot be cast '''
        def cast_list_to_float(string):
            return [float(x) for x in overlay_result if HelperUtils.get_type(x)==FLOAT and "." in x]

        def cast_list_to_int(string):
            return [int(x) for x in overlay_result if HelperUtils.get_type(x)==INT or HelperUtils.get_type(x)==FLOAT]

        def cast_list_to_str(string):
            _list = [str(x).strip() for x in overlay_result if HelperUtils.get_type(x)==TEXT]

            # convert all string elements to lowercase, for case-insensitive comparison tests
            return list(map(lambda x: x.lower(), _list))

        _list = []
        if HelperUtils.get_type(value)==INT:
            _list = cast_list_to_int(value)
        elif HelperUtils.get_type(value)==FLOAT:
            _list = cast_list_to_float(value)
        else:
            _list = cast_list_to_str(value)

        return _list



    def get_overlay_result(self, column_name):
        ''' Return overlay result for given column/attribute from the gdf 
            Returns --> list
        '''
        overlay_result = []
        try:
            overlay_result = self.overlay_gdf[column_name].tolist()
        except KeyError as e:
            layer_name = self.cddp_question['layer_name']
            _list = HelperUtils.pop_list(self.overlay_gdf.columns.to_list())
            logger.error(f'Property Name "{column_name}" not found in layer "{layer_name}".\nAvailable properties are "{_list}".')

        return overlay_result

    def comparison_result(self):
        '''
        value from 'CDDP Admin' is type str - the correct type must be determined and then cast to numerical/str at runtime for comparison operator
        operators => ['IsNull', 'IsNotNull', 'GreaterThan', 'LessThan', 'Equals']

        Returns --> list
        '''
        try:

            column_name   = self.cddp_question.get('column_name')
            operator   = self.cddp_question.get('operator')
            value      = str(self.cddp_question.get('value'))
            value_type = HelperUtils.get_type(value)
            overlay_result = self.get_overlay_result(column_name)

            #import ipdb; ipdb.set_trace()
            cast_overlay_result = self.cast_list(value, overlay_result)
            if len(overlay_result) == 0:
                # list is empty
                pass
            if operator == 'IsNull': 
                # TODO
                pass
            else:
                if operator == 'IsNotNull':
                    # list is not empty
                    self.operator_result = ','.join(overlay_result)

                elif operator == GREATER_THAN:
                    self.operator_result = [str(x) for x in overlay_result if x > float(value)]

                elif operator == LESS_THAN:
                    self.operator_result = [str(x) for x in overlay_result if x < float(value)]

                elif operator == EQUALS:
                    if value_type != TEXT and value in overlay_result:
                        self.operator_result = [str(x) for x in overlay_result]
                    elif value.lower().strip() in cast_overlay_result:
                        # comparing strings
                        self.operator_result = overlay_result

            #self.operator_result = self.operator_result if isinstance(self.operator_result, list) else [self.operator_result]
            self.operator_result = self.operator_result if isinstance(self.operator_result, list) else self.operator_result.split(',')
            return self.operator_result
        except ValueError as e:
            logger.error(f'Error casting to INT or FLOAT: Overlay Result {overlay_result}\n \
                           Layer column_name: "lga_label", operator: "GreaterThan", value: "19500000"\n{str(e)}')
        except Exception as e:
            logger.error(f'Error determining operator result: Overlay Result {overlay_result}, Operator {operator}, Value {value}\{str(e)}')

        #import ipdb; ipdb.set_trace()
        return self.operator_result


    def proponent_answer(self):
        """
        Answer to be prefilled for proponent
        """

        #import ipdb; ipdb.set_trace()
        proponent_text = []

        if self.widget_type not in TEXT_WIDGETS:
            return None

        visible_to_proponent = self.cddp_question.get('visible_to_proponent', False)
        proponent_answer = self.cddp_question.get('answer', '').strip()
        prefix_answer = self.cddp_question.get('prefix_answer', '').strip()
        no_polygons_proponent = self.cddp_question.get('no_polygons_proponent', -1)

        if not visible_to_proponent:
            _str = proponent_answer + ' ' + prefix_answer if '::' not in proponent_answer else prefix_answer
            return _str.strip()

        #import ipdb; ipdb.set_trace()
        if proponent_answer:
            if '::' in proponent_answer:
                column_name = proponent_answer.split('::')[1].strip()
                if column_name != self.cddp_question['column_name'].strip():
                    proponent_text = self.get_overlay_result(column_name)
                else:
                    proponent_text = self.operator_result
            else:
                proponent_text = proponent_answer

        if no_polygons_proponent >= 0:
            # extract the result from the first 'no_polygons_proponent' polygons only
            proponent_text = proponent_text[:no_polygons_proponent]

        if proponent_text and isinstance(proponent_text, list) and isinstance(proponent_text[0], str):
            proponent_text = ', '.join(proponent_text)

        if prefix_answer:
            # text to be inserted always at beginning of an answer text
            proponent_text = prefix_answer + ' ' + proponent_text if proponent_text else prefix_answer

        return proponent_text
        
    def assessor_answer(self):
        """
        Answer to be prefilled for assessor
        """

        assessor_text = []
        assessor_info = self.cddp_question.get('assessor_info', '').strip()
        prefix_info = self.cddp_question.get('prefix_info', '').strip()
        no_polygons_assessor = self.cddp_question.get('no_polygons_assessor', -1)

        if assessor_info:
            # assessor must see this response instead of overlay response answer
            if '::' in assessor_info:
                column_name = assessor_info.split('::')[1].strip()
                if column_name != self.cddp_question['column_name'].strip():
                    assessor_text = self.get_overlay_result(column_name)
                else:
                    assessor_text = self.operator_result
            else:
                assessor_text = assessor_info

        if no_polygons_assessor >= 0:
            # extract the result from the first 'no_polygons_proponent' polygons
            assessor_text = assessor_text[:no_polygons_assessor]

        #import ipdb; ipdb.set_trace()
        if assessor_text and isinstance(assessor_text, list) and isinstance(assessor_text[0], str):
            assessor_text = ', '.join(assessor_text)

        if prefix_info:
            # text to be inserted always at beginning of an answer text
            assessor_text = prefix_info + ' ' + assessor_text if assessor_text else prefix_info

        return assessor_text

 

