from django.contrib.gis.geos import GEOSGeometry, Polygon, MultiPolygon
from django.conf import settings
from django.db import transaction
from django.core.cache import cache
from rest_framework import status

import pandas as pd
import geopandas as gpd
import requests
import json
import os
import io
import pytz
import traceback
from datetime import datetime
import time

from sqs.components.gisquery.models import Layer #, Feature#, LayerHistory
from sqs.utils.loader_utils import DbLayerProvider
from sqs.utils.helper import (
    DefaultOperator,
    #HelperUtils,
    #pop_list,
)
from sqs.utils import (
    TEXT_WIDGETS,
    RADIOBUTTONS,
    CHECKBOX,
    MULTI_SELECT,
    SELECT,
)

from sqs.utils import HelperUtils
from sqs.exceptions import LayerProviderException

import logging
logger = logging.getLogger(__name__)

DATE_FMT = '%Y-%m-%d'
DATETIME_FMT = '%Y-%m-%d %H:%M:%S'

RESPONSE_LEN = 75


class DisturbanceLayerQueryHelper():

    def __init__(self, masterlist_questions, geojson, proposal):
        self.masterlist_questions = masterlist_questions
        self.geojson = self.read_geojson(geojson)
        self.proposal = proposal
        self.unprocessed_questions = []
        self.metrics = []

    def read_geojson(self, geojson):
        """ geojson is the user specified polygon, used to intersect the layers """
        try:
            mpoly = gpd.read_file(json.dumps(geojson))
        except Exception as e:
            raise Exception(f'Error reading geojson file: {str(e)}')

        return mpoly

    def add_buffer(self, cddp_question):
        '''
        Converts Polar Projection from EPSG:xxxx (eg. EPSG:4326) in deg to Cartesian Projection (in meters),
        add buffer (in meters) to the new projection, then reverts the buffered polygon to 
        the original projection

        Input: buffer_size -- in meters

        Returns the the original polygon, perimeter increased by the buffer size
        '''
        mpoly = self.geojson
        if 'POLYGON' not in str(mpoly):
            logger.warn(f'Proposal ID {self.proposal.get("id")}: Uploaded Shapefile/Polygon is NOT a POLYGON\n {mpoly}.')

        try:
            buffer_size = cddp_question['buffer']
            if buffer_size:
                crs_orig =  mpoly.crs.srs

                # convert to new projection so that buffer can be added in meters
                mpoly_cart = mpoly.to_crs(settings.CRS_CARTESIAN)
                mpoly_cart['geometry'] = mpoly_cart['geometry'].buffer(buffer_size)

                # revert to original projection
                mpoly_buffer = mpoly_cart.to_crs(crs_orig)

                return mpoly_buffer
            
        except Exception as e:
            logger.error(f'Error adding buffer {buffer_size} to polygon for CDDP Question {cddp_question}.\n{e}')
            
        return mpoly

    def overlay_how(self, how):
        """
        overlay.how options available (in geopandas) => ['interesection', 'union', 'difference', 'symmetrical difference']
                            supported (in SQS)       => ['interesection', 'difference']
        """
        if how=='Overlapping':
            return 'intersection'
        elif how=='Outside':
            #return 'difference'
            return 'symmetric_difference'
        else:
            logger.error(f'Error: Unknown "how" operator: {how}')

#    def get_unique_layer_list(self):
#        unique_layer_list = []
#        for question_group in self.masterlist_questions:
#            for question in question_group['questions']:
#                _dict = dict(layer_name=question['layer_name'], layer_url=question['layer_url'])
#                if _dict not in unique_layer_list:
#                    unique_layer_list.append(_dict)
#        return unique_layer_list

    def get_attributes(self, layer_gdf):
        cols = layer_gdf.columns.drop(['id','md5_rowhash', 'geometry'])
        attrs = layer_gdf[cols].to_dict(orient='records')
        #return layer_gdf[cols].to_dict(orient='records')

        # drop duplicates
        attrs = pd.DataFrame(attrs).drop_duplicates().to_dict('r')
        return attrs

    def get_grouped_questions(self, question):
        """
        Return the entire question group. 
        For example, given a radiobutton or checkbox question, return the all question/answer combinations for that question
        """

#        def reorder_mlq():
#            ''' Reorders the masterlist_questions in the question_group, sorting layer_names '''
#            ordered_question_group = []
#            ordered_multiple_question_group = []
#    
#            mlq_unique_layers = list(set([question_group['questions'][0]['layer']['layer_name'] for question_group in self.masterlist_questions]))
#            for unique_layer_name in mlq_unique_layers:
#                for question_group in self.masterlist_questions:
#                    #for question in question_group['questions']:
#                    first_question = question_group['questions'][0]
#                    layer_name = first_question['layer']['layer_name']
#                    if layer_name == unique_layer_name:
#                        if len(question_group['questions']) == 1:
#                            ordered_question_group.append(question_group)
#                        else:
#                            ordered_multiple_question_group.append(question_group)
#    
#            return ordered_question_group + ordered_multiple_question_group

        def reorder_question_group():
            ''' Reorders the questions in the question_group , sorting layer_names. Sort Nested Multiples Questions by layer_name'''
            ordered_question_group = dict(question_group=question_group['question_group'])
            ordered_questions = []
            for unique_layer_name in unique_layers:
                for question in question_group['questions']:
                    layer_name = question['layer']['layer_name']
                    if layer_name == unique_layer_name:
                        ordered_questions.append(question)
            ordered_question_group.update(dict(questions=ordered_questions))
            return ordered_question_group

        try:
            #reordered_masterlist_questions = reorder_mlq()
            #for question_group in reordered_masterlist_questions:
            #for question_group in reordered_masterlist_questions:
            #    print(len(question_group['questions']), question_group['questions'][0]['layer']['layer_name'], question_group['question_group'][:30])

            #for question_group in reordered_masterlist_questions:
            for question_group in self.masterlist_questions:
                if question_group['question_group'] == question:
                    #return question_group
                    unique_layers = list(set([question['layer']['layer_name'] for question in question_group['questions']]))
                    if len(unique_layers) > 1:
                        return reorder_question_group()
                    return question_group

        except Exception as e:
            logger.error(f'Error searching for question_group: \'{question}\'\n{e}')

        return []

    def set_metrics(self, cddp_question, layer_provider, expired, condition, time_retrieve_layer, time_taken, error):
        self.metrics.append(
            dict(
                question=cddp_question['question'],
                answer_mlq=cddp_question['answer_mlq'],
                expired=expired,
                layer_name=layer_provider.layer_name,
                layer_cached=layer_provider.layer_cached,
                condition=condition,
                time_retrieve_layer=round(time_retrieve_layer, 3),
                time=round(time_taken, 3),
                error=f'{error}',
                result=None,
                assessor_answer=None,
                operator_response=None,
            )
        )
        return self.metrics

    def spatial_join_gbq(self, question, widget_type):
        '''
        Process new Question (grouping by like-questions) and results stored in cache 

        NOTE: All questions for the given layer 'layer_name' will be processed by 'spatial_join()' and results stored in cache. 
              This will save time reloading and querying layers for questions from the same layer_name. 
              It is CPU cost effective to query all questions for the same layer now, and cache results for 
              subsequent potential question/answer queries.
        '''

        try:
            error_msg = ''
            today = datetime.now(pytz.timezone(settings.TIME_ZONE))
            response = []
            layer_info = {}
            expired = False

            grouped_questions = self.get_grouped_questions(question)
            if len(grouped_questions)==0:
                return response

            for cddp_question in grouped_questions['questions']:
                start_time = time.time()

                question_expiry = datetime.strptime(cddp_question['expiry'], DATE_FMT).date() if cddp_question['expiry'] else None
                if question_expiry is None or question_expiry >= today.date():
                 
                    start_time_retrieve_layer = time.time()
                    layer_name = cddp_question['layer']['layer_name']
                    layer_url = cddp_question['layer']['layer_url']

                    answer_str = f'A: \'{cddp_question.get("answer_mlq")[:25]}\'' if cddp_question.get('answer_mlq') else ''
                    logger.info('---------------------------------------------------------------------------------------------')
                    logger.info(f'Proposal ID {self.proposal["id"]}: Processing Question \'{cddp_question.get("question")[:25]}\' {answer_str} ...')
      
                    if layer_name != layer_info.get('layer_name'):
                        # layer name not available in memory - retrieve/re-retrieve
                        layer_provider = DbLayerProvider(layer_name, url=layer_url)
                        layer_info, layer_gdf = layer_provider.get_layer()
                    else:
                        logger.info(f'Layer {layer_name} already in memory ...')

                    time_retrieve_layer = time.time() - start_time_retrieve_layer

                    how = cddp_question['how']
                    column_name = cddp_question['column_name']
                    operator = cddp_question['operator']
                    value = cddp_question['value']

        #            if cddp_question['question']=='1.0 Proposal title':
        #                pass

                    how = self.overlay_how(how) # ['interesection', 'difference']

                    mpoly = self.add_buffer(cddp_question)
                    if layer_gdf.crs.srs.lower() != mpoly.crs.srs.lower():
                        mpoly.to_crs(layer_gdf.crs.srs, inplace=True)

                    # For overlay function, how='symmetric_difference' is the opposite of 'intersection'. To get 'symmetetric_difference' we will
                    # compute 'intersection' and filter the intersected features from the layer_gdf.
                    # That is, fo both cases of 'intersection' or 'symmetrical_difference' - we need to calc 'intersection'
                    overlay_gdf = layer_gdf.overlay(mpoly, how='intersection', keep_geom_type=False)

#                    # filter layer intersections with very low area/boundary_length ratios
#                    overlay_cart_gdf = overlay_gdf.to_crs(settings.CRS_CARTESIAN)
#                    overlay_cart_gdf = overlay_cart_gdf[[(overlay_cart_gdf.area/overlay_cart_gdf.length > settings.GEOM_AREA_LENGTH_FILTER) & ((overlay_cart_gdf.area/overlay_cart_gdf.length).isna())]]
#                    overlay_gdf = overlay_cart_gdf.to_crs(layer_gdf.crs.srs)

                    if column_name not in overlay_gdf.columns:
                        _list = HelperUtils.pop_list(overlay_gdf.columns.to_list())
                        error_msg = f'Property Name "{column_name}" not found in layer "{layer_name}".\nAvailable properties are "{_list}".'
                        logger.error(error_msg)

                    if how == 'intersection':
                        # already computed above
                        pass
                    else:
                        # equivalent to 'symmetrical difference', but re-introducing very low area/boundary_length ratios, features which would otherwise be omitted
                        overlay_gdf = layer_gdf[~layer_gdf[column_name].isin( overlay_gdf[column_name].unique() )]

                    # operators ['IsNull', 'IsNotNull', 'GreaterThan', 'LessThan', 'Equals']
                    op = DefaultOperator(cddp_question, overlay_gdf, widget_type)
                    operator_result = op.operator_result()
                    logger.info(f'Operator Result: {operator_result}')
                    condition = f'{column_name} -- {operator}'
                    if operator != 'IsNotNull':
                        condition += f' -- {value}'

                    res = dict(
                            question=cddp_question['question'],
                            answer=cddp_question['answer_mlq'],
                            visible_to_proponent=cddp_question['visible_to_proponent'],
                            layer_details = dict(**layer_info,
                                sqs_timestamp=today.strftime(DATETIME_FMT),
                                error_msg = error_msg,
                            ),
                            condition=[how, condition],
                            operator_response=operator_result if isinstance(operator_result, list) else [operator_result],
                            proponent_answer=op.proponent_answer(),
                            assessor_answer=op.assessor_answer(),
                            add_info_section_prop=cddp_question['show_add_info_section_prop'],
                        )
                    response.append(res)
                else:
                    logger.warn(f'Expired {question_expiry}: Ignoring question {cddp_question}')
                    expired = True

                self.set_metrics(cddp_question, layer_provider, expired, condition, time_retrieve_layer, time.time() - start_time, error=None)
                logger.info(f'Time Taken: {round(time.time() - start_time, 3)} secs')

        except Exception as e: 
            logger.error(e)
            self.set_metrics(cddp_question, layer_provider, expired, condition, time_retrieve_layer, time.time() - start_time, error=e)

        return response

    def get_processed_question(self, question, widget_type):
        ''' Gets or Sets processed (spatial_join executed) question from cache 
            NOTE: processed questions caching not implemented
        '''
        processed_questions = []
        try:
            processed_questions = self.spatial_join_gbq(question, widget_type)
        except Exception as e:
            logger.error(traceback.print_exc())
            logger.error(f'Error Searching Question combination in SQS Cache/Spatial Join: \'{question}\'\n{e}')

        return processed_questions

    def query_question(self, item, answer_type):

        def set_metric_result(response):
            ''' Adds result from the intersection (and label) to metrics'''
            try:
                if 'layer_details' in response:
                    for layer_detail in response['layer_details']:
                        question = layer_detail['question']['question']
                        answer_mlq = layer_detail['question']['answer']
                        for idx, metric in enumerate(self.metrics):
                            if metric['question']==question and metric['answer_mlq']==answer_mlq:
                                if response['result']:
                                    metric.update({'result': response['result']})
                                else:
                                    proponent_answer = layer_detail['question']['proponent_answer']
                                    metric.update({'result': proponent_answer})

                                assessor_answer = layer_detail['question']['assessor_answer']
                                operator_response = ', '.join(map(str, layer_detail['question']['operator_response']))
                                operator_response = operator_response[:RESPONSE_LEN] + ' ...' if len(operator_response)>RESPONSE_LEN else operator_response
                                metric.update({'assessor_answer': assessor_answer})
                                metric.update({'operator_response': operator_response})
            except Exception as e:
                logger.warn(f'Could not add result to Metrics\n{e}')


        #start_time = time.time()
        response = {}
        if answer_type == RADIOBUTTONS:
            response = self.find_radiobutton(item)

        elif answer_type == CHECKBOX:
            response = self.find_checkbox(item)

        elif answer_type == MULTI_SELECT:
            response = self.find_multiselect(item)

        elif answer_type == SELECT:
            response = self.find_select(item)

        elif answer_type == TEXT_WIDGETS:
            response = self.find_other(item)

        set_metric_result(response)
        #self.total_query_time += time.time() - start_time
        return response

    def find_radiobutton(self, item):
        ''' Widget --> radiobutton
            1. question['operator_response']  --> contains results from SQS intersection and equality comparison
            2. Iterate through item_options (from proposal.schema) and compare with question['answer']

            If item_options==question['answer'] && len(question['operator_response'])>0, then return rb as checked

            result --> result (str, returns first label from list of labels that match operator_response)
        '''
        response = {}
        question = {}
        try:
            schema_question  = item['label']
            schema_section = item['name']
            item_options   = item['options']

            processed_questions = self.get_processed_question(schema_question, widget_type=item['type'])
            if len(processed_questions)==0:
                return {}

            layer_details=[]
            for item in item_options:
                label = item['label']
                value = item['value']
                # return first checked radiobutton in order rb's appear in 'item_option_labels' (schema question)
                for question in processed_questions:
                    if label.casefold() == question['answer'].casefold() and len(question['operator_response'])>0:

                        raw_data = question
                        details = raw_data.pop('layer_details', None)

                        response =  dict(
                            result=label,
                            assessor_info=[],
                            layer_details=[dict(name=schema_section, label=value, details=details, question=question)],
                        )
                        if label or value:
                            # return first match found
                            return response

        except Exception as e:
            logger.error(f'RADIOBUTTON: Searching Question in SQS processed_questions dict: \'{question}\'\n{e}')

        return response

    def find_checkbox(self, item):
        ''' Widget --> checkbox
            1. question['operator_response']  --> contains results from SQS intersection and equality comparison
            2. Iterate through item_options (from proposal.schema) and compare with question['answer']

            If item_options==question['answer'] && len(question['operator_response'])>0, then return cb as checked

            result --> result (list, list of labels that match operator_response)
        '''
        response = {}
        question = {}
        try:
            schema_question = item['label']
            item_options    = item['children']

            item_options_dict = [dict(name=i['name'], label=i['label']) for i in item_options]
            processed_questions = self.get_processed_question(schema_question, widget_type=item['type'])
            if len(processed_questions)==0:
                return {}

            result=[]
            layer_details=[]
            for _d in item_options_dict:
                name = _d['name']
                label = _d['label']
                for question in processed_questions:
                    if label.casefold() == question['answer'].casefold() and len(question['operator_response'])>0:

                        result.append(label) # result is in an array list 
                        raw_data = question
                        details = raw_data.pop('layer_details', None)
                        # [lbl] - next line 'list' hack for disturbance/components/proposals/api.py 'refresh()' method, when only a single checkbox is selected
                        layer_details.append(dict(name=name, label=[label], details=details, question=raw_data))

            response =  dict(
                result=result,
                assessor_info=[],
                layer_details=layer_details,
            )

        except Exception as e:
            logger.error(f'CHECKBOX: Searching Question in SQS processed_questions dict: \'{question}\'\n{e}')

        return response

    def find_select(self, item):
        ''' Widget --> select
            1. question['operator_response']  --> contains results from SQS intersection and equality comparison
            
            result --> result (str, first item in sorted list of labels that match operator_response)
        '''
        response = {}
        question = {}
        try:
            schema_question  = item['label']
            schema_section = item['name']
            item_options   = item['options']

            processed_questions = self.get_processed_question(schema_question, widget_type=item['type'])
            if len(processed_questions) != 1:
                # for multi-select questions, there must be only one question
                logger.error(f'SELECT: For select question, there must be only one question, {len(processed_questions)} found: \'{question}\'')
                return {}
            question = processed_questions[0]

            item_labels = [i['label'] for i in item_options] # these are the available answer options proponent can choose from
            operator_response = question['operator_response'] # these are the answers from the query intersection/difference (truncated to no. of polygons/answers to return)

            # return only those labels that are in the available choices to the proponent
            # case-insensitive intersection. returns labels found in both lists
            labels_found = list({str.casefold(x) for x in item_labels} & {str.casefold(x) for x in operator_response})
            #labels_found = [str.casefold(x) for x in operator_response]
            labels_found.sort()

            raw_data = question
            details = raw_data.pop('layer_details', None)
            if len(labels_found)>0:
                result = labels_found[0] # return the first one found
                response =  dict(
                    result=result, # returns str
                    #assessor_info=[question['assessor_answer']],
                    assessor_info=[],
                    layer_details=[dict(name=schema_section, label=result, details=details, question=question)]
                )

        except Exception as e:
            logger.error(f'SELECT: Searching Question in SQS processed_questions dict: \'{question}\'\n{e}')

        return response

    def find_multiselect(self, item):
        ''' Widget --> multi-select
            1. question['operator_response']  --> contains results from SQS intersection and equality comparison

            result --> result (list of labels that match operator_response)
        '''
        response = {}
        question = {}
        try:
            schema_question  = item['label']
            schema_section = item['name']
            item_options   = item['options']

            processed_questions = self.get_processed_question(schema_question, widget_type=item['type'])
            if len(processed_questions) != 1:
                # for multi-select questions, there must be only one question
                logger.error(f'MULTI-SELECT: For multi-select question, there must be only one question, {len(processed_questions)} found: \'{question}\'')
                return {}
            question = processed_questions[0]

            item_labels = [i['label'] for i in item_options] # these are the available answer options proponent can choose from
            operator_response = question['operator_response'] # these are the answers from the query intersection/difference (truncated to no. of polygons/answers to return)

            # return only those labels that are in the available choices to the proponent
            # case-insensitive intersection. returns labels found in both lists
            labels_found = list({str.casefold(x) for x in item_labels} & {str.casefold(x) for x in operator_response})
            #labels_found = [str.casefold(x) for x in operator_response]
            labels_found.sort()

            raw_data = question
            details = raw_data.pop('layer_details', None)
            if labels_found:
                result = list(set(labels_found))
                response =  dict(
                    result=result,
                    #assessor_info=[question['assessor_answer']],
                    assessor_info=[],
                    layer_details=[dict(name=schema_section, label=result, details=details, question=question)]
                )

        except Exception as e:
            logger.error(f'MULTI-SELECT: Searching Question in SQS processed_questions dict: \'{question}\'\n{e}')

        return response

    def find_other(self, item):
        ''' Widget --> text, text_area
            Iterate through spatial join response and return all items retrieved by spatial join method, that also 
            exists in item_options (from proposal.schema)

            Returns --> str 
        '''
        response = {}
        question = {}
        try:
            schema_question = item['label']
            schema_section  = item['name']
            schema_label    = schema_question

            processed_questions = self.get_processed_question(schema_question, widget_type=item['type'])
            if len(processed_questions)==0:
                return {}

            layer_details=[]
            if len(processed_questions)>0:
                question = processed_questions[0] 
                details = question.pop('layer_details', None)
                label = question['proponent_answer'] if question['proponent_answer'] else None
                response =  dict(
                    #assessor_info = question['assessor_answer'],
                    result=label,
                    assessor_info = question['assessor_answer'],
                    layer_details=[dict(name=schema_section, label=label, details=details, question=question)]
                )

        except LayerProviderException as e:
            raise LayerProviderException(str(e))
        except Exception as e:
            logger.error(f'SELECT: Searching Question in SQS processed_questions dict: \'{question}\'\n{e}')

        return response


class PointQueryHelper():
    """
    pq = PointQueryHelper('cddp:dpaw_regions', ['region','office'], 121.465836, -30.748890)
    pq.spatial_join()
    """

    def __init__(self, layer_name, layer_attrs, longitude, latitude):
        self.layer_name = layer_name
        self.layer_attrs = layer_attrs
        self.longitude = longitude
        self.latitude = latitude

    def spatial_join(self, predicate='within'):

        layer = Layer.objects.get(name=self.layer_name)
        layer_gdf = layer.to_gdf

        # Lat Long for Kalgoolie, Goldfields
        # df = pd.DataFrame({'longitude': [121.465836], 'latitude': [-30.748890]})
        # settings.CRS = 'EPSG:4236'
        df = pd.DataFrame({'longitude': [self.longitude], 'latitude': [self.latitude]})
        point_gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs=settings.CRS)

        overlay_res = gpd.sjoin(point_gdf, layer_gdf, predicate=predicate)

        attrs_exist = all(item in overlay_res.columns for item in self.layer_attrs)

        if attrs_exist:
            errors = None
            if len(self.layer_attrs)==0 or overlay_res.empty:
                # no attrs specified - so return them all
                layer_attrs = overlay_res.drop('geometry', axis=1).columns
            elif len(self.layer_attrs)>0 and attrs_exist:
                # return only requested attrs
                layer_attrs = self.layer_attrs 
            else: #elif not attrs_exist:
                # one or more attr requested not found in layer - return all attrs and error message
                layer_attrs = overlay_res.drop('geometry', axis=1).columns
                errors = f'Attribute(s) not available: {self.layer_attrs}. Attributes available in layer: {list(layer_attrs.array)}'

            #layer_attrs = self.layer_attrs if len(self.layer_attrs)>0 and attrs_exist else overlay_res.drop('geometry', axis=1).columns
            overlay_res = overlay_res.iloc[0] if not overlay_res.empty else overlay_res # convert row to pandas Series (removes index)

            try: 
                res = dict(status=status.HTTP_200_OK, name=self.layer_name, errors=errors, res=overlay_res[layer_attrs].to_dict() if not overlay_res.empty else None)
            except Exception as e:
                logger.error(e)
                res = dict(status=status.HTTP_400_BAD_REQUEST, name=self.layer_name, error=str(e), res=overlay_res.to_dict() if not overlay_res.empty else None)
        else:
            layer_attrs = overlay_res.drop('geometry', axis=1).columns
            errors = f'Attribute(s) not available: {self.layer_attrs}. Attributes available in layer: {list(layer_attrs.array)}'
            res = dict(status=status.HTTP_400_BAD_REQUEST, name=self.layer_name, errors=errors, res=None)

        return res

