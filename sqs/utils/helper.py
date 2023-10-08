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
        self.row_filter = self._comparison_result()

    def _cast_list(self, value, overlay_result):
        ''' cast all array items to value type, discarding thos that cannot be cast '''
        def cast_list_to_float(string):
            return [float(x) for x in overlay_result if HelperUtils.get_type(x)==FLOAT and "." in str(x)]

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



    def _get_overlay_result(self, column_name):
        ''' Return (filtered) overlay result for given column/attribute from the gdf 
            self.row_filter contains row indexes of overlay_gdf that match the operator_compare criteria
            Returns --> list
        '''
        overlay_result = []
        try:
            overlay_gdf = self.overlay_gdf.iloc[self.row_filter,:] if self.row_filter is not None else self.overlay_gdf
            overlay_result = overlay_gdf[column_name].tolist()
        except KeyError as e:
            layer_name = self.cddp_question['layer']['layer_name']
            _list = HelperUtils.pop_list(self.overlay_gdf.columns.to_list())
            logger.error(f'Property Name "{column_name}" not found in layer "{layer_name}".\nAvailable properties are "{_list}".')

        return overlay_result # return unique values
        #return list(set(overlay_result)) # return unique values

    def _comparison_result(self):
        '''
        value from 'CDDP Admin' is type str - the correct type must be determined and then cast to numerical/str at runtime for comparison operator
        operators => ['IsNull', 'IsNotNull', 'GreaterThan', 'LessThan', 'Equals']

        Returns --> list of geo dataframe row indices where comparison ooperator returned True.
                    This list is used to filter to original self.overlay_gdf.
        '''
        try:

            column_name   = self.cddp_question.get('column_name')
            operator   = self.cddp_question.get('operator')
            value      = str(self.cddp_question.get('value'))
            value_type = HelperUtils.get_type(value)

            self.row_filter = None
            overlay_result = self._get_overlay_result(column_name)

            cast_overlay_result = self._cast_list(value, overlay_result)
            if len(overlay_result) == 0:
                # list is empty
                pass
            if operator == 'IsNull': 
                # TODO
                pass
            else:
                if operator == 'IsNotNull':
                    # list is not empty
                    self.row_filter = [idx for idx,x in enumerate(overlay_result) if str(x).strip() != '']

                elif operator == GREATER_THAN:
                    self.row_filter = [idx for idx,x in enumerate(overlay_result) if x > float(value)]

                elif operator == LESS_THAN:
                    self.row_filter = [idx for idx,x in enumerate(overlay_result) if x < float(value)]

                elif operator == EQUALS:
                    if value_type != TEXT:
                        # cast to INTs then compare (ignore decimals in comparison). int(x) will truncate x.
                        self.row_filter = [idx for idx,x in enumerate(overlay_result) if int(float(x))==int(float(value))]
                    else:
                        # comparing strings
                        self.row_filter = [idx for x in enumerate(overlay_result) if str(x).lower().strip()==value.lower().strip()]

            return self.row_filter
        except ValueError as e:
            logger.error(f'Error casting to INT or FLOAT: Overlay Result {overlay_result}\n \
                           Layer column_name: {column_name}, operator: {operator}, value: {value}\n{str(e)}')
        except Exception as e:
            logger.error(f'Error determining operator result: Overlay Result {overlay_result}, Operator {operator}, Value {value}\{str(e)}')

        return self.row_filter


    def operator_result(self):
        '''
        summary of query results - filters
        '''
        column_name   = self.cddp_question.get('column_name')
        _operator_result = self._get_overlay_result(column_name)
        #import ipdb; ipdb.set_trace()
        #return _operator_result
        return list(set(_operator_result))

    def proponent_answer(self):
        """
        Answer to be prefilled for proponent
        """

        proponent_text = []
        visible_to_proponent = self.cddp_question.get('visible_to_proponent', False)
        proponent_answer = self.cddp_question.get('answer', '').strip()
        prefix_answer = self.cddp_question.get('prefix_answer', '').strip()
        no_polygons_proponent = self.cddp_question.get('no_polygons_proponent', -1)

        if not visible_to_proponent:
            _str = prefix_answer + ' ' + proponent_answer if '::' not in proponent_answer else prefix_answer
            return _str.strip()

        if proponent_answer:
            if '::' in proponent_answer:
                column_name = proponent_answer.split('::')[1].strip()
                proponent_text = list(set(self._get_overlay_result(column_name)))
            else:
                proponent_text = proponent_answer

        #import ipdb; ipdb.set_trace()
        if no_polygons_proponent >= 0:
            # extract the result from the first 'no_polygons_proponent' polygons only
            proponent_text = proponent_text[:no_polygons_proponent]

        if self.widget_type in TEXT_WIDGETS:
        #if True:
            # Return text string for TEXT WIDGETS

            # perform additional processing and convert list to str (otherwise return list)
            if proponent_text and isinstance(proponent_text, list) and isinstance(proponent_text[0], str):
                proponent_text = ', '.join(proponent_text)

            if prefix_answer:
                # text to be inserted always at beginning of an answer text
                proponent_text = prefix_answer + ' ' + proponent_text if proponent_text else prefix_answer

        #import ipdb; ipdb.set_trace()
        return proponent_text
        #return proponent_text if isinstance(proponent_text, list) else [proponent_text]
        #return list(set(proponent_text))
        
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
                assessor_text = list(set(self._get_overlay_result(column_name)))
            else:
                assessor_text = assessor_info

        if no_polygons_assessor >= 0:
            # extract the result from the first 'no_polygons_proponent' polygons
            assessor_text = assessor_text[:no_polygons_assessor]

        if assessor_text and isinstance(assessor_text, list) and isinstance(assessor_text[0], str):
            assessor_text = ', '.join(assessor_text)

        if prefix_info:
            # text to be inserted always at beginning of an answer text
            assessor_text = prefix_info + ' ' + assessor_text if assessor_text else prefix_info

        return assessor_text
        #return list(set(assessor_text))

 

