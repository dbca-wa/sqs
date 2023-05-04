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

import logging
logger = logging.getLogger(__name__)

DATE_FMT = '%Y-%m-%d'
DATETIME_FMT = '%Y-%m-%d %H:%M:%S'


class DisturbanceLayerQueryHelper():

    def __init__(self, masterlist_questions, geojson, proposal):
        self.masterlist_questions = masterlist_questions
        self.geojson = self.read_geojson(geojson)
        self.proposal = proposal
        #self.processed_questions = []
        self.unprocessed_questions = []

    def read_geojson(self, geojson):
        """ geojson is the user specified polygon, used to intersect the layers """
        mpoly = gpd.read_file(json.dumps(geojson))
        if mpoly.crs.srs != settings.CRS:
            # CRS = 'EPSG:4236'
            mpoly.to_crs(settings.CRS, inplace=True)

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
                #import ipdb; ipdb.set_trace()
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

#    def get_layer(self, layer_name):
#        '''
#        Get Layer Objects from cache if exists, otherwise get from DB and set the cache
#        '''
#        # try to get from cached 
#        layer_gdf = cache.get(self.LAYER_CACHE.format(layer_name))
#        layer_info = cache.get(self.LAYER_DETAILS_CACHE.format(layer_name))
#          
#        if layer_gdf is None:
#            layer = Layer.objects.get(name=layer_name)
#            layer_gdf = layer.to_gdf
#            if layer_gdf.crs.srs != settings.CRS:
#                layer_gdf.to_crs(settings.CRS, inplace=True)
#
#            layer_info = dict(
#                layer_name=layer_name,
#                layer_created=layer.created_date.strftime(DATETIME_FMT),
#                layer_version=layer.version,
#            )
#
#            # set the cache 
#            cache.set(self.LAYER_CACHE.format(layer_name), layer_gdf, settings.CACHE_TIMEOUT)
#            cache.set(self.LAYER_DETAILS_CACHE.format(layer_name), layer_info, settings.CACHE_TIMEOUT)
#
#        #import ipdb; ipdb.set_trace()
#        return layer_info, layer_gdf

    def get_attributes(self, layer_gdf):
        cols = layer_gdf.columns.drop(['id','md5_rowhash', 'geometry'])
        attrs = layer_gdf[cols].to_dict(orient='records')
        #return layer_gdf[cols].to_dict(orient='records')

        # drop duplicates
        attrs = pd.DataFrame(attrs).drop_duplicates().to_dict('r')
        return attrs

#    def spatial_join(self, question):
#        '''
#        Grouped By Layer
#
#        Process new layer (all questions for layer_name will be processed) and results stored in cache 
#
#        NOTE: All questions for the given layer 'layer_name' will be processed by 'spatial_join()' and results stored in cache. 
#              This will save time reloading and querying layers for questions from the same layer_name. 
#              It is CPU cost effective to query all questions for the same layer now, and cache results for 
#              subsequent potential question/answer queries.
#        '''
#
#        #response = [] 
#        #now = datetime.now().date()
#        #import ipdb; ipdb.set_trace()
#        today = datetime.now(pytz.timezone(settings.TIME_ZONE))
#
#        #import ipdb; ipdb.set_trace()
#        questions_grouped_by_layer = self.get_questions_grouped_by_layer(question)
#        layer_name = questions_grouped_by_layer.get('layer_name')
#        layer = Layer.objects.get(name=layer_name)
#        layer_gdf = layer.to_gdf
#        if layer_gdf.crs.srs != settings.CRS:
#            layer_gdf.to_crs(settings.CRS, inplace=True)
#
#        #for cddp_question in self.get_questions_grouped_by_layer(question):
#        for cddp_question in questions_grouped_by_layer['questions']:
#            column_name = cddp_question['column_name']
#            operator = cddp_question['operator']
#            how = cddp_question['how']
#            expiry = datetime.strptime(cddp_question['expiry'], DATE_FMT).date() if cddp_question['expiry'] else None
#
##            if cddp_question['question']=='1.0 Proposal title':
##                #import ipdb; ipdb.set_trace()
##                pass
#
#            how = self.overlay_how(how) # ['interesection', 'difference']
#
#            overlay_res = layer_gdf.overlay(self.geojson, how=how)
#            try:
#                res = overlay_res[column_name].tolist()
#            except KeyError as e:
#                _list = pop_list(overlay_res.columns.to_list())
#                logger.error(f'Property Name "{column_name}" not found in layer "{layer_name}".\nAvailable properties are "{_list}".')
#
#            # operators ['IsNull', 'IsNotNull', 'GreaterThan', 'LessThan', 'Equals']
#            ret = operator_result(cddp_question, res)
#
#            #import ipdb; ipdb.set_trace()
#            response = dict(
#                    question=cddp_question['question'],
#                    answer=cddp_question['answer_mlq'],
#                    expired=False if (expiry and expiry > today.date()) or not expiry else True,
#                    visible_to_proponent=cddp_question['visible_to_proponent'],
#                    #proponent_answer=proponent_answer(cddp_question, ret),
#                    #assessor_answer=assessor_answer(cddp_question, ret),
#                    layer_name=layer_name,
#                    layer_sqs_created=layer.created.strftime(DATETIME_FMT),
#                    layer_updated=today.strftime(DATETIME_FMT),
#                    #response=ret if isinstance(ret, list) else ret.tolist()
#                    response=ret if isinstance(ret, list) else [ret]
#                )
#            #)
#            #if not already_exists(question, answer, layer_name):
#            #    self.processed_questions.append(response)
#            self.processed_questions.append(response)
#
#        return response

    def get_grouped_questions(self, question):
        """
        Return the entire question group. 
        That is, find the layer_name to which question belong then return all questions in that layer group.
        """
        try:
            #import ipdb; ipdb.set_trace()
            #expiry = datetime.strptime('2023-01-01', '%Y-%m-%d').date()
            #now = datetime.now().date()
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
        #response = [] 
        #now = datetime.now().date()
        today = datetime.now(pytz.timezone(settings.TIME_ZONE))
        response = []

        #import ipdb; ipdb.set_trace()
        grouped_questions = self.get_grouped_questions(question)
        if len(grouped_questions)==0:
            return response
        #question = grouped_questions.get('question_group')

        for cddp_question in grouped_questions['questions']:

            question_expiry = datetime.strptime(cddp_question['expiry'], DATE_FMT).date()
            if question_expiry > today.date():
  
                layer_name = cddp_question['layer_name']
                layer_url = cddp_question['layer_url']
                #layer_info, layer_gdf = self.get_layer(layer_name)
                layer_info, layer_gdf = DbLayerProvider(layer_name, url=layer_url).get_layer()

                #import ipdb; ipdb.set_trace()

                column_name = cddp_question['column_name']
                operator = cddp_question['operator']
                how = cddp_question['how']
                expiry = datetime.strptime(cddp_question['expiry'], DATE_FMT).date() if cddp_question['expiry'] else None

    #            if cddp_question['question']=='1.0 Proposal title':
    #                #import ipdb; ipdb.set_trace()
    #                pass

                how = self.overlay_how(how) # ['interesection', 'difference']

                #import ipdb; ipdb.set_trace()
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

                #import ipdb; ipdb.set_trace()
                res = dict(
                        question=cddp_question['question'],
                        answer=cddp_question['answer_mlq'],
                        #expired=False if (expiry and expiry > today.date()) or not expiry else True,
                        visible_to_proponent=cddp_question['visible_to_proponent'],
                        proponent_answer=operator.proponent_answer(),
                        assessor_answer=operator.assessor_answer(),
                        layer_details = dict(**layer_info,
                            sqs_timestamp=today.strftime(DATETIME_FMT),
                            #attrs = self.get_attributes(overlay_gdf),
                            error_msg = error_msg,
                        ),
                        response=operator_result if isinstance(operator_result, list) else [operator_result],
                    )
                response.append(res)
                #self.processed_questions.append(res)
            else:
                logger.warn(f'Expired {question_expiry}: Ignoring question {cddp_question}')

        return response

    def get_processed_question(self, question, widget_type):
        ''' Gets or Sets processed (spatial_join executed) question from cache '''
        #import ipdb; ipdb.set_trace()
        processed_questions = []
        try:
            processed_questions = self.spatial_join_gbq(question, widget_type)
        except Exception as e:
            logger.error(traceback.print_exc())
            logger.error(f'Error Searching Question comination in SQS Cache/Spatial Join: \'{question}\'\n{e}')

        return processed_questions

    def find_radiobutton(self, item):
        ''' Widget --> radiobutton
            Iterate through spatial join response and return FIRST radiobutton item retrieved by spatial join method, that also 
            exists in item_options (from proposal.schema)

            NOTE: Each radiobutton component has multiple questions and multiple defined layers, one for each radiobutton. 
                  Last one may be omitted, this will be assumed the default, if any other radiobutton is not selected.
                  (CDDP Question, answer_mlq is required)
            (Test Proposal --> http://localhost:8003/external/proposal/1518)

            Returns --> str
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
            #import ipdb; ipdb.set_trace()
            for label in item_option_labels:
                for question in processed_questions:
                    if label == question['answer'] and len(question['response'])>0:
                        res.append(label) # result is in an array list
                        #import ipdb; ipdb.set_trace()
                        if question['assessor_answer'] not in assessor_info:
                            assessor_info.append(question['assessor_answer'])

            #import ipdb; ipdb.set_trace()
            # If no res resturned from spatial_join then check if default option is available
            if len(res)==0:
                cddp_answers = [i['answer'] for i in processed_questions] # all answers from the cddp for this question's radiobuttons
                try:
                    #import ipdb; ipdb.set_trace()
                    default = set(item_option_labels).difference(cddp_answers)
                except Exception as e:
                    import ipdb; ipdb.set_trace()
                    pass

                selected = list(default)[0]

                if len(default) > 1:
                    # msg notifying that system is returning the first default option found
                    no_radiobuttons = len(item_option_labels)
                    logger.warn(f'RADIOBUTTONS: has too many defaults options. There are {no_radiobuttons} radiobuttons in \
                                  proposal.schema. There should be {no_radiobuttons-1} CDDP Question(s) in MasterList - found \
                                  {no_radiobuttons - len(default)} CDDP Question(S) in ML.\nReturning first default {selected} \
                                  from possible {list(default)}')

                res.append(selected)
                if question['assessor_answer'] not in assessor_info:
                    #import ipdb; ipdb.set_trace()
                    assessor_info.append(question['assessor_answer'])

            #import ipdb; ipdb.set_trace()
            response =  dict(
                result=res[0] if len(res)>0 else None,
                assessor_info=assessor_info,
                layer_details=[dict(name=schema_section, label=label, details=question.get('layer_details'))],
            )

        except Exception as e:
            import ipdb; ipdb.set_trace()
            logger.error(f'RADIOBUTTON: Searching Question in SQS processed_questions dict: \'{question}\'\n{e}')

        return response

    def find_select(self, item):
        ''' Widget --> select
            Iterate through spatial join response and return all items retrieved by spatial join method, that also 
            exists in item_options (from proposal.schema)

            NOTE: Each multi-select component has a single question and a single defined layer (Each select option DOES NOT have individual layers)
                  (CDDP Question, answer_mlq is required)
            (Test Proposal --> http://localhost:8003/external/proposal/1520)

            Returns --> str
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
            assessor_info=[]
            layer_details=[]
            question = {}
            #import ipdb; ipdb.set_trace()
            for _d in item_options_dict:
                label = _d['label']

                for question in processed_questions:
                    if label == question['answer'] and len(question['response'])>0:
                        result = label # result is in an array list
                        if question['assessor_answer'] not in assessor_info:
                            assessor_info.append(question['assessor_answer'])

            if result:
                response =  dict(
                    result=result,
                    assessor_info=assessor_info,
                    layer_details=[dict(name=schema_section, label=None, details=question.get('layer_details'))],
                )

        except Exception as e:
            logger.error(f'SELECT: Searching Question in SQS processed_questions dict: \'{question}\'\n{e}')

        return response

    def find_multiselect(self, item):
        ''' Widget --> multi-select
            Iterate through spatial join response and return all items retrieved by spatial join method, that also 
            exists in item_options (from proposal.schema)

            NOTE: Each multi-select component has a single question and a single defined layer (the multi-select options DO NOT have individual layers)
            (Test Proposal --> http://localhost:8003/external/proposal/1521[22])

            Returns --> list
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

            #import ipdb; ipdb.set_trace()
            result=[]
            assessor_info=[]
            layer_details=[]
            question = {}
            item_options_dict = [dict(label=i['label']) for i in item_options]
            for _d in item_options_dict:
                label = _d['label']

                for question in processed_questions:
                    if label == question['answer'] and len(question['response'])>0:
                        result.append(get_value(label)) # result is in an array list
                        if question['assessor_answer'] not in assessor_info:
                            assessor_info.append(question['assessor_answer'])

            response =  dict(
                result=result,
                assessor_info=assessor_info,
                layer_details=[dict(name=schema_section, label=None, details=question.get('layer_details'))],
            )

        except Exception as e:
            import ipdb; ipdb.set_trace()
            logger.error(f'MULTI-SELECT: Searching Question in SQS processed_questions dict: \'{question}\'\n{e}')

        return response


    def find_checkbox(self, item):
        ''' Widget --> checkbox
            Iterate through spatial join response and return all items retrieved by spatial join method, that also 
            exists in item_options (from proposal.schema)

            NOTE: Each checkbox component has multiple questions and multiple defined layers
                  (CDDP Question, answer_mlq is required)
            (Test Proposal --> http://localhost:8003/external/proposal/1519)

            Returns --> list
        '''
        response = {}
        try:
            #import ipdb; ipdb.set_trace()
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
                    if label == question['answer'] and len(question['response'])>0:
                        result.append(label) # result is in an array list 
                        layer_details.append(dict(name=name, label=None, details=question.get('layer_details')))
                        if question['assessor_answer'] not in assessor_info:
                            assessor_info.append(question['assessor_answer'])
 
#                    if len(question['response'])>0:
#                        if question['answer'] and ',' in question['answer']:
#                            # comma-delimited answer in answer_mlq variable
#                            answers = [i.strip() for i in question['answer'].split(',')]
#                            for answer in answers:
#                                if label == answer:
#                                    res.append(label)
#                                    layer_details.append(dict(name=name, label=None, details=question['layer_details']))
#
#                        elif label == question['answer']:
#                            res.append(label)
#                            layer_details.append(dict(name=name, label=None, details=question['layer_details']))


            response =  dict(
                result=result,
                assessor_info=assessor_info,
                layer_details=layer_details,
            )

        except Exception as e:
            logger.error(f'CHECKBOX: Searching Question in SQS processed_questions dict: \'{question}\'\n{e}')

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

            #import ipdb; ipdb.set_trace()
            processed_questions = self.get_processed_question(schema_question, widget_type=item['type'])
            if len(processed_questions)==0:
                return {}

            layer_details=[]
            question = {}
            if len(processed_questions)>0:
                question = processed_questions[0] 
                response =  dict(
                    assessor_info = dict(
                        proponent_answer=question['proponent_answer'],
                        assessor_answer=question['assessor_answer'],
                    ),
                    layer_details=[dict(name=schema_section, label=None, details=question.get('layer_details'))]
                )

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

        #import ipdb; ipdb.set_trace()
        response = [] 
        response2 = {} 
        proponent_single = []
        assessor_single = []
        #now = datetime.now().date()
        today = datetime.now(pytz.timezone(settings.TIME_ZONE))

        for data in self.cddp_info:
            layer_name = data['layer_name']

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

        #import ipdb; ipdb.set_trace()
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

