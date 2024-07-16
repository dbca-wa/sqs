from collections import OrderedDict
import json
import fnmatch
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
CONTAINS     = 'Contains'
LIKE         = 'Like'
OR           = 'OR'
ISNOTNULL    = 'IsNotNull'
ISNULL       = 'IsNull'


class DefaultOperator():
    '''
        layer => layer details for given cddp_question (possibly multiple layers per question)
        overlay_gdf   => overlay gdf from gpd.overlay (intersection, difference etc)    --> GeoDataFrame
    '''
    def __init__(self, layer, overlay_gdf, widget_type):
        self.layer = layer
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

    def _get_overlay_result_df(self, column_names):
        ''' Return (filtered) overlay result gdf for given columns/attributes from the gdf 
            self.row_filter contains row indexes of overlay_gdf that match the operator_compare criteria
            Returns --> gdf
        '''
        overlay_result = []
        try:
            overlay_gdf = self.overlay_gdf.iloc[self.row_filter,:] if self.row_filter is not None else self.overlay_gdf
            overlay_result_df = overlay_gdf[column_names]
        except KeyError as e:
            layer_name = self.layer['layer']['layer_name']
            _list = HelperUtils.pop_list(self.overlay_gdf.columns)
            logger.error(f'Property Name "{column_names}" not found in layer "{layer_name}".\nAvailable properties are "{_list}".')

        return overlay_result_df

    def _get_overlay_result(self, column_name):
        ''' Return (filtered) overlay result for given column/attribute from the gdf 
            self.row_filter contains row indexes of overlay_gdf that match the operator_compare criteria
            Returns --> list
        '''
        return self._get_overlay_result_df(column_name).to_list()

    def _comparison_result(self):
        '''
        value from 'CDDP Admin' is type str - the correct type must be determined and then cast to numerical/str at runtime for comparison operator
        operators => ['IsNull', 'IsNotNull', 'GreaterThan', 'LessThan', 'Equals']

        fnmatch example:
            fnmatch.filter(['kimberley', 'midwest', 'pilbara', 'pilbara', 'swan', 'swan'], '*imb*'.lower())
            --> ['kimberley']

            fnmatch.filter(['kimberley', 'midwest', 'pilbara', 'pilbara', 'swan', 'swan'], '*e?*'.lower())
            --> ['kimberley', 'midwest']

        Returns --> list of geo dataframe row indices where comparison ooperator returned True.
                    This list is used to filter to original self.overlay_gdf.
        '''

        def get_filtered_idxs(pattern):
            overlay_result_lower = list(map(lambda x: str(x).lower(), overlay_result))
            #pattern = '*' + value.lower().strip().strip('*') + '*'
            overlay_result_match = fnmatch.filter(overlay_result_lower, pattern)

            if NOT_DIFFERENCE:
                # Contains NOT
                overlay_result_match = list(set(overlay_result_lower).difference(overlay_result_match))

            # get index positions of found results in ORIG overlay_result list
            return [overlay_result_lower.index(x) for x in overlay_result_match]


        try:

            NOT_DIFFERENCE = False
            column_name   = self.layer.get('column_name')
            operator   = self.layer.get('operator')
            value      = str(self.layer.get('value'))
            if value.startswith('!'):
                value = value.strip('!')
                NOT_DIFFERENCE = True

            value_type = HelperUtils.get_type(value)

            self.row_filter = None
            overlay_result = self._get_overlay_result(column_name)

            cast_overlay_result = self._cast_list(value, overlay_result)
            if len(overlay_result) == 0:
                # list is empty
                pass
            if operator == ISNULL: 
                # TODO
                pass
            else:
                if operator == ISNOTNULL:
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
                        self.row_filter = [idx for idx,x in enumerate(overlay_result) if str(x).lower().strip()==value.lower().strip()]

                elif operator == CONTAINS:
                    pattern = '*' + value.lower().strip().strip('*') + '*'
                    self.row_filter = get_filtered_idxs(pattern)

                elif operator == LIKE:
                    pattern = value.lower().strip()
                    self.row_filter = get_filtered_idxs(pattern)

                elif operator == OR:
                    overlay_result_lower = list(map(lambda x: str(x).lower(), overlay_result))
                    values_list = list(map(lambda x: str(x).lower().strip(), value.split('|')))
                    overlay_result_match = list(set(overlay_result_lower).intersection(values_list))

                    if NOT_DIFFERENCE:
                        # OR NOT
                        overlay_result_match = list(set(overlay_result_lower).difference(overlay_result_match))

                    # get index positions of found results in ORIG overlay_result list
                    self.row_filter = [overlay_result_lower.index(x) for x in overlay_result_match]

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
        column_name   = self.layer.get('column_name')
        _operator_result = self._get_overlay_result(column_name)
        return list(set(_operator_result))

    def proponent_answer(self):
        visible_to_proponent = self.layer.get('visible_to_proponent', False)
        proponent_items = self.layer.get('proponent_items')
        column_names = [i['answer'] for i in proponent_items if 'answer' in i and i['answer']]
        column_prefix = [i['prefix'] for i in proponent_items if 'prefix' in i and i['prefix']]

        grouped_res = []
        if visible_to_proponent:
            grouped_res = self._get_overlay_result_df(column_names).to_csv(header=None, index=False).strip('\n').split('\n')

        if column_prefix:
            grouped_res = [column_prefix[0]]  + grouped_res

        return '\n'.join(grouped_res).replace(',',', ').replace('\\n', '\n')

    def assessor_answer(self):
        assessor_items = self.layer.get('assessor_items')
        column_names = [i['answer'] for i in assessor_items if 'answer' in i and i['answer']]
        column_prefix = [i['prefix'] for i in assessor_items if 'prefix' in i and i['prefix']]

        grouped_res = self._get_overlay_result_df(column_names).to_csv(header=None, index=False).strip('\n').split('\n')

        if column_prefix:
            grouped_res = [column_prefix[0]]  + grouped_res

        return '\n'.join(grouped_res).replace(',',', ').replace('\\n', '\n')


