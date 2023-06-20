from django.contrib.gis.geos import GEOSGeometry, Polygon, MultiPolygon
from django.conf import settings
from django.db import transaction
from django.core.cache import cache

import pandas as pd
import geopandas as gpd
import requests
import json
import os
import io
import pytz
import traceback
from datetime import datetime

from sqs.components.gisquery.models import Layer #, Feature#, LayerHistory
from sqs.utils.loader_utils import DbLayerProvider
from sqs.utils.helper import (
    DefaultOperator,
    #HelperUtils,
    #pop_list,
)
from sqs.utils import HelperUtils
from sqs.exceptions import LayerProviderException

import logging
logger = logging.getLogger(__name__)

DATE_FMT = '%Y-%m-%d'
DATETIME_FMT = '%Y-%m-%d %H:%M:%S'


class DisturbanceLayerQueryHelper():

    def __init__(self, masterlist_questions, geojson, proposal):
        self.masterlist_questions = masterlist_questions
        self.geojson = self.read_geojson(geojson)
        self.proposal = proposal
        self.unprocessed_questions = []

    def read_geojson(self, geojson):
        """ geojson is the user specified polygon, used to intersect the layers """
        try:
            mpoly = gpd.read_file(json.dumps(geojson))
            if mpoly.crs.srs != settings.CRS:
                # CRS = 'EPSG:4236'
                mpoly.to_crs(settings.CRS, inplace=True)
        except Exception as e:
            raise Exception(f'Error reading geojson file: {str(e)}')

        return mpoly

    def add_buffer(self, cddp_question):
        '''
        Converts Polar Projection from EPSG:4326 (in deg) to Cartesian Projection (in meters),
        add buffer (in meters) to the new projection, then reverts the buffered polygon to 
        the original projection

        Input: buffer_size -- in meters

        Returns the the original polygon, perimeter increased by the buffer size
        '''
        mpoly = self.geojson
        try:
            buffer_size = cddp_question['buffer']
            if buffer_size:
                buffer_size = float(buffer_size) 
                if mpoly.crs.srs != settings.CRS:
                    mpoly.to_crs(settings.CRS, inplace=True)

                # convert to new projection so that buffer can be added in meters
                mpoly_cart = mpoly.to_crs(settings.CRS_CARTESIAN)
                mpoly_cart_buffer = mpoly_cart.buffer(buffer_size)
                mpoly_cart_buffer_gdf = gpd.GeoDataFrame(geometry=mpoly_cart_buffer)

                # revert to original projection
                mpoly_buffer = mpoly_cart_buffer_gdf.to_crs(settings.CRS)

                mpoly = mpoly_buffer
            
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
            return 'difference'
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
        That is, find the layer_name to which question belong then return all questions in that layer group.
        """
        try:
            for question_group in self.masterlist_questions:
                if question_group['question_group'] == question:
                    return question_group

        except Exception as e:
            logger.error(f'Error searching for question_group: \'{question}\'\n{e}')

        return []

    def spatial_join_gbq(self, question, widget_type):
        '''
        Process new Question (grouping by like-questions) and results stored in cache 

        NOTE: All questions for the given layer 'layer_name' will be processed by 'spatial_join()' and results stored in cache. 
              This will save time reloading and querying layers for questions from the same layer_name. 
              It is CPU cost effective to query all questions for the same layer now, and cache results for 
              subsequent potential question/answer queries.
        '''

        error_msg = ''
        today = datetime.now(pytz.timezone(settings.TIME_ZONE))
        response = []

        grouped_questions = self.get_grouped_questions(question)
        if len(grouped_questions)==0:
            return response

        for cddp_question in grouped_questions['questions']:

            question_expiry = datetime.strptime(cddp_question['expiry'], DATE_FMT).date()
            if question_expiry > today.date():
  
                layer_name = cddp_question['layer']['layer_name']
                layer_url = cddp_question['layer']['layer_url']
                layer_info, layer_gdf = DbLayerProvider(layer_name, url=layer_url).get_layer()

                column_name = cddp_question['column_name']
                operator = cddp_question['operator']
                how = cddp_question['how']
                expiry = datetime.strptime(cddp_question['expiry'], DATE_FMT).date() if cddp_question['expiry'] else None

    #            if cddp_question['question']=='1.0 Proposal title':
    #                #import ipdb; ipdb.set_trace()
    #                pass

                how = self.overlay_how(how) # ['interesection', 'difference']

                mpoly = self.add_buffer(cddp_question)
                #mpoly = self.geojson
                overlay_gdf = layer_gdf.overlay(mpoly, how=how)
                try:
                    res = overlay_gdf[column_name].tolist()
                except KeyError as e:
                    _list = HelperUtils.pop_list(overlay_gdf.columns.to_list())
                    error_msg = f'Property Name "{column_name}" not found in layer "{layer_name}".\nAvailable properties are "{_list}".'
                    logger.error(error_msg)

                # operators ['IsNull', 'IsNotNull', 'GreaterThan', 'LessThan', 'Equals']
                operator = DefaultOperator(cddp_question, overlay_gdf, widget_type)
                operator_result = operator.comparison_result()

                res = dict(
                        question=cddp_question['question'],
                        answer=cddp_question['answer_mlq'],
                        #expired=False if (expiry and expiry > today.date()) or not expiry else True,
                        visible_to_proponent=cddp_question['visible_to_proponent'],
                        proponent_answer=operator.proponent_answer(),
                        assessor_answer=operator.assessor_answer(),
                        layer_details = dict(**layer_info,
                            #question=cddp_question['question'],
                            #answer=cddp_question['answer_mlq'],
                            sqs_timestamp=today.strftime(DATETIME_FMT),
                            #attrs = self.get_attributes(overlay_gdf),
                            error_msg = error_msg,
                        ),
                        operator_response=operator_result if isinstance(operator_result, list) else [operator_result],
                    )
                response.append(res)
            else:
                logger.warn(f'Expired {question_expiry}: Ignoring question {cddp_question}')

        return response

    def get_processed_question(self, question, widget_type):
        ''' Gets or Sets processed (spatial_join executed) question from cache '''
        processed_questions = []
        try:
            processed_questions = self.spatial_join_gbq(question, widget_type)
        except Exception as e:
            logger.error(traceback.print_exc())
            logger.error(f'Error Searching Question comination in SQS Cache/Spatial Join: \'{question}\'\n{e}')

        return processed_questions

    def find_radiobutton(self, item):
        ''' Widget --> radiobutton
            1. question['operator_response']  --> contains results from SQS intersection and equality comparison
            2. Iterate through item_options (from proposal.schema) and compare with question['answer']

            If item_options==question['answer'] && len(question['operator_response'])>0, then return rb as checked
        '''
        response = {}
        try:
            schema_question  = item['label']
            schema_section = item['name']
            item_options   = item['options']

            item_option_labels = [i['label'] for i in item_options]
            processed_questions = self.get_processed_question(schema_question, widget_type=item['type'])
            if len(processed_questions)==0:
                return {}

            res=[]
            assessor_info=[]
            layer_details=[]
            question = {}
            details = {}
            sqs_data = {}
            for label in item_option_labels:
                # return first checked radiobutton in order rb's appear in 'item_option_labels' (schema question)
                for question in processed_questions:
                    if label.casefold() == question['answer'].casefold() and len(question['operator_response'])>0:
                        res.append(label) # result is in an array list
                        raw_data = question
                        details = raw_data.pop('layer_details', None)
                        layer_details.append(dict(name=schema_section, label=label, details=details, question=raw_data))

#                        if question['assessor_answer'] not in assessor_info:
#                            raw_data = question
#                            details = raw_data.pop('layer_details', None)
#                            #assessor_info.append(question['assessor_answer'])
#                            layer_details.append(dict(name=schema_section, label=label, details=details, question=raw_data))

                        response =  dict(
                            result=res[0] if len(res)>0 else None,
                            assessor_info=assessor_info,
                            #layer_details=[dict(name=schema_section, label=label, details=details, question=raw_data)],
                            layer_details=layer_details,
                        )
                        return response

        except Exception as e:
            logger.error(f'RADIOBUTTON: Searching Question in SQS processed_questions dict: \'{question}\'\n{e}')

        return response

    def find_checkbox(self, item):
        ''' Widget --> checkbox
            1. question['operator_response']  --> contains results from SQS intersection and equality comparison
            2. Iterate through item_options (from proposal.schema) and compare with question['answer']

            If item_options==question['answer'] && len(question['operator_response'])>0, then return cb as checked
        '''
        response = {}
        try:
            schema_question = item['label']
            item_options    = item['children']

            item_options_dict = [dict(name=i['name'], label=i['label']) for i in item_options]
            processed_questions = self.get_processed_question(schema_question, widget_type=item['type'])
            if len(processed_questions)==0:
                return {}

            result=[]
            assessor_info=[]
            layer_details=[]
            question = {}
            for _d in item_options_dict:
                name = _d['name']
                label = _d['label']
                for question in processed_questions:
                    if label.casefold() == question['answer'].casefold() and len(question['operator_response'])>0:
                        result.append(label) # result is in an array list 
                        raw_data = question
                        details = raw_data.pop('layer_details', None)
                        layer_details.append(dict(name=name, label=label, details=details, question=raw_data))
#                        response.update(
#                            dict(
#                                result=label,
#                                assessor_info=assessor_info,
#                                layer_details=layer_details,
#                            )
#                        )

            response =  dict(
                result=result,
                assessor_info=assessor_info,
                layer_details=layer_details,
            )

        except Exception as e:
            logger.error(f'CHECKBOX: Searching Question in SQS processed_questions dict: \'{question}\'\n{e}')

        return response

    def find_select(self, item):
        ''' Widget --> select
            1. question['operator_response']  --> contains results from SQS intersection and equality comparison

            If len(question['operator_response'])>0, then return select item as checked
        '''
        response = {}
        try:
            schema_question  = item['label']
            schema_section = item['name']
            item_options   = item['options']

            item_options_dict = [dict(label=i['label']) for i in item_options]
            processed_questions = self.get_processed_question(schema_question, widget_type=item['type'])
            if len(processed_questions)==0:
                return {}

            result = []
            layer_details=[]
            question = processed_questions[0]
            details = {}
            for _d in item_options_dict:
                label = _d['label']

                if label.casefold() == question['answer'].casefold() and len(question['operator_response'])>0:

                    details = question.pop('layer_details', None)
                    response =  dict(
                        result=label,
                        assessor_info=[question['assessor_answer']],
                        #layer_details=[dict(name=schema_section, label=None, details=details, question=raw_data)],
                        layer_details=[dict(name=schema_section, label=label, details=details, question=question)],
                    )
                    return response

        except Exception as e:
            logger.error(f'SELECT: Searching Question in SQS processed_questions dict: \'{question}\'\n{e}')

        return response

    def find_multiselect(self, item):
        ''' Widget --> multi-select
            1. question['operator_response']  --> contains results from SQS intersection and equality comparison

            If len(question['operator_response'])>0, then return multi-selects item as checked
        '''
        def get_value(label):
            ''' get the label value from list of dicts eg. [{'label': 'CITY OF JOONDALUP', 'value': 'CITY-OF-JOONDALUP'}] '''
            for item in item_options:
                if item['label'] == label:
                    return item['value']
            return None

        response = {}
        try:
            schema_question  = item['label']
            schema_section = item['name']
            item_options   = item['options']

            processed_questions = self.get_processed_question(schema_question, widget_type=item['type'])
            if len(processed_questions)==0:
                return {}

            result=[]
            layer_details=[]
            question = [] #processed_questions[0]
            item_options_dict = [dict(label=i['label']) for i in item_options]
            for _d in item_options_dict:
                label = _d['label']

                for question in processed_questions:
                    if label.casefold() == question['answer'].casefold() and len(question['operator_response'])>0:
                        result.append(get_value(label)) # result is in an array list
                        raw_data = question
                        details = raw_data.pop('layer_details', None)
                        layer_details.append(dict(name=schema_section, label=get_value(label), details=details, question=raw_data))

            response =  dict(
                result=result,
                assessor_info=[question['assessor_answer']],
                #layer_details=[dict(name=schema_section, label=None, details=details, question=question)],
                layer_details=layer_details,
            )

        except Exception as e:
            logger.error(f'MULTI-SELECT: Searching Question in SQS processed_questions dict: \'{question}\'\n{e}')

        return response


    def find_other(self, item):
        ''' Widget --> text, text_area
            Iterate through spatial join response and return all items retrieved by spatial join method, that also 
            exists in item_options (from proposal.schema)
            (Test Proposal --> http://localhost:8003/external/proposal/1525)

            Returns --> str
        '''
        response = {}
        try:
            schema_question = item['label']
            schema_section  = item['name']
            schema_label    = schema_question

            processed_questions = self.get_processed_question(schema_question, widget_type=item['type'])
            if len(processed_questions)==0:
                return {}

            layer_details=[]
            question = {}
            if len(processed_questions)>0:
                question = processed_questions[0] 
                details = question.pop('layer_details', None)
                label = question['proponent_answer'] if question['proponent_answer'] else None
                response =  dict(
#                    assessor_info = dict(
#                        proponent_answer=question['proponent_answer'],
#                        assessor_answer=question['assessor_answer'],
#                    ),
                    assessor_info = question['assessor_answer'],
                    layer_details=[dict(name=schema_section, label=label, details=details, question=question)]
                )

        except LayerProviderException as e:
            raise LayerProviderException(str(e))
        except Exception as e:
            logger.error(f'SELECT: Searching Question in SQS processed_questions dict: \'{question}\'\n{e}')

        return response

class LayerQuerySingleHelper():

    def __init__(self, question, widget_type, cddp_info, geojson):
        self.question = question
        self.widget_type = widget_type
        self.cddp_info = cddp_info
        self.geojson = self.read_geojson(geojson)

    def read_geojson(self, geojson):
        """ geojson is the use specified polygon, used to intersect the layers """
        mpoly = gpd.read_file(json.dumps(geojson))
        if mpoly.crs.srs != settings.CRS:
            # CRS = 'EPSG:4236'
            mpoly.to_crs(settings.CRS, inplace=True)

        return mpoly

    def spatial_join(self):

        response = [] 
        response2 = {} 
        proponent_single = []
        assessor_single = []
        #now = datetime.now().date()
        today = datetime.now(pytz.timezone(settings.TIME_ZONE))

        for data in self.cddp_info:
            layer_name = data['layer']['layer_name']

            column_name = data['column_name']
            operator = data['operator']
            how = data['how']
            expiry = datetime.strptime(data['expiry'], DATE_FMT).date() if data['expiry'] else None
            
            layer = Layer.objects.get(name=layer_name)
            layer_gdf = layer.to_gdf
            if layer_gdf.crs.srs != settings.CRS:
                layer_gdf.to_crs(settings.CRS, inplace=True)
    
            how = self.overlay_how(how) # ['interesection', 'difference']


            # add buffer to user polygon

            overlay_res = layer_gdf.overlay(self.geojson, how=how)
            try:
                res = overlay_res[column_name].values
            except KeyError as e:
                _list = HelperUtils.pop_list(overlay_res.columns.to_list())
                logger.error(f'Property Name "{column_name}" not found in layer "{layer_name}".\nAvailable properties are "{_list}".')
                continue

            # operators ['IsNull', 'IsNotNull', 'GreaterThan', 'LessThan', 'Equals']
            ret = operator_result(data, res)

            proponent_single.append(proponent_answer(data, self.widget_type, ret))
            assessor_single.append(assessor_answer(data, self.widget_type, ret))

        response2 =   dict(
            question=self.question,
            widget_type=self.widget_type,
            proponent_answer=proponent_single,
            assessor_answer=assessor_single,
        )

        return response2


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
            res = dict(name=self.layer_name, errors=errors, res=overlay_res[layer_attrs].to_dict() if not overlay_res.empty else None)
        except Exception as e:
            logger.error(e)
            res = dict(name=self.layer_name, error=str(e), res=overlay_res.to_dict() if not overlay_res.empty else None)

        return res

