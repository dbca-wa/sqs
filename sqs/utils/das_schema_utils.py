import traceback
import os
import json

from sqs.utils.geoquery_utils import DisturbanceLayerQueryHelper
#from sqs.utils.helper  import SchemaSearch
from sqs.exceptions import LayerProviderException

import logging
logger = logging.getLogger(__name__)


class DisturbanceLayerQuery(object):

    def __init__(self, masterlist_questions, geojson, proposal):
        self.lq_helper = DisturbanceLayerQueryHelper(masterlist_questions, geojson, proposal)
        self.prefill_obj = DisturbancePrefillData(self.lq_helper)

    def query(self):
        self.lq_helper.processed_questions = []
        self.lq_helper.unprocessed_questions = []

        prefill_data = self.prefill_obj.prefill_data_from_shape()

        res = dict(
            system='DAS',
            data=prefill_data,
            layer_data=self.prefill_obj.layer_data,
            add_info_assessor=self.prefill_obj.add_info_assessor,
        )
        return res


class DisturbancePrefillData(object):
    """
    from disturbance.components.proposals.utils import PrefillData
    pr=PrefillData()
    pr.prefill_data_from_shape(p.schema)
    """

    def __init__(self, layer_query_helper):
        self.layer_query_helper = layer_query_helper
        orig_data = self.layer_query_helper.proposal.get('data')

        self.data = {}
        self.layer_data = []
        self.add_info_assessor = {}

    def prefill_data_from_shape(self):
        schema = self.layer_query_helper.proposal.get('schema')

        try:
            for item in schema:
                self.data.update(self._populate_data_from_item(item, 0, ''))
        except:
            traceback.print_exc()
        return [self.data]

    def _populate_data_from_item(self, item, repetition, suffix, sqs_value=None):

        item_data = {}

        if isinstance(item, dict) and 'name' in item:
            extended_item_name = item['name']
        else:
            raise Exception(f'Missing name in item {item["label"]}. Possibly Question/Section not provided!')

        if 'children' not in item:
            if item['type'] =='checkbox':
                if sqs_value:
                    for val in sqs_value:
                        if val==item['label']:
                            item_data[item['name']]='on'

            elif item['type'] == 'file':
                #print('file item', item)
                pass
            else:
                    if item['type'] == 'multi-select':
                        #Get value from SQS. Value should be an array of the correct options.
                        
                        # don't overwrite if propsal['data'] already has a value set
                        sqs_dict = self.layer_query_helper.find_multiselect(item)
                        sqs_values = sqs_dict.get('result')
                        
                        if sqs_values:
                            self._update_assessor_info(item, sqs_dict)
                            self._update_layer_info(sqs_dict)

                            # Next Line: resetting to None before refilling - TODO perhaps run for all within __init__()
                            item_data[item['name']]=[]

                            for val in sqs_values:
                                if item['options']:
                                    for op in item['options']:
                                        if val==op['value']:
                                            item_data[item['name']].append(op['value'])
                                            #sqs_assessor_value='test'

                    elif item['type'] in ['radiobuttons', 'select']:
                        #Get value from SQS
                        if item['type'] == 'select':
                            sqs_dict = self.layer_query_helper.find_select(item)
                        elif item['type'] == 'radiobuttons':
                            sqs_dict = self.layer_query_helper.find_radiobutton(item)

                        sqs_value = sqs_dict.get('result')
                        layer_details = sqs_dict.get('layer_details')
                        if sqs_value:
                            self._update_assessor_info(item, sqs_dict)
                            self._update_layer_info(sqs_dict)

                            if item['options']:
                                for op in item['options']:
                                    #if sqs_value==op['value']:
                                    if sqs_value==op['label']:
                                        item_data[item['name']]=op['value']
                                        break

                    elif item['type'] in ['text', 'text_area']:
                        #All the other types e.g. text_area, text, date (except label).
                        if item['type'] != 'label':
                            sqs_dict = self.layer_query_helper.find_other(item)
                            #import ipdb; ipdb.set_trace()
                            assessor_info = sqs_dict.get('assessor_info')
                            if sqs_dict.get('layer_details'):
                                item_data[item['name']] = sqs_dict.get('layer_details')[0]['label']
                            self._update_layer_info(sqs_dict)

                            #if sqs_values:
                            if assessor_info:
                                self._update_assessor_info(item, sqs_dict)
                                #self._update_layer_info(sqs_dict)
                    else:
                        #All the other types e.g. date, number etc (except label).
                        pass
        else:
            if 'repetition' in item:
                item_data = self.generate_item_data_shape(extended_item_name,item,item_data,1,suffix)
            else:
                #Check if item has checkbox childer
                if self.check_checkbox_item(extended_item_name, item, item_data,1,suffix):
                    #make a call to sqs for item
                    # 1. question      --> item['label']
                    # 2. checkbox text --> item['children'][0]['label']
                    # 3. request response for all checkbox's ie. send item['children'][all]['label']. 
                    #    SQS will return a list of checkbox's answersfound eg. ['National park', 'Nature reserve']

                    sqs_dict = self.layer_query_helper.find_checkbox(item)
                    sqs_values = sqs_dict.get('result')
                    if sqs_values:
                        self._update_assessor_info(item, sqs_dict)
                        item_layer_data = self._update_layer_info(sqs_dict)
                        item_data = self.generate_item_data_shape(extended_item_name, item, item_data,1,suffix, sqs_values)
                else:
                    item_data = self.generate_item_data_shape(extended_item_name, item, item_data,1,suffix)


        if 'conditions' in item:
            try: 
                for condition in item['conditions'].keys():
                    if item_data and condition==item_data[item['name']]:
                        for child in item['conditions'][condition]:
                            item_data.update(self._populate_data_from_item(child,  repetition, suffix))
            except Exception as e:
                logger.error(f'Error "conditions": {str(e)}')

        return item_data

    def generate_item_data_shape(self, item_name,item,item_data,repetition,suffix, sqs_value=None):
        item_data_list = []
        for rep in range(0, repetition):
            child_data = {}
            for child_item in item.get('children'):
                child_data.update(self._populate_data_from_item(child_item, 0,
                                                         '{}-{}'.format(suffix, rep), sqs_value))
                #print('child item in generate item data', child_item)
            item_data_list.append(child_data)

            item_data[item['name']] = item_data_list
        return item_data

    def check_checkbox_item(self, item_name,item,item_data,repetition,suffix):
        checkbox_item=False
        for child_item in item.get('children'):
            if child_item['type']=='checkbox':
                checkbox_item=True        
        return checkbox_item

    def _update_layer_info(self, sqs_dict):
        layer_info = []

        try:
            layer_details = sqs_dict.get('layer_details', [])
            for ld in layer_details:
                self.layer_data.append(
                    dict(
                        name=ld['name'] if 'name' in ld else None,
                        response=ld['label'] if 'label' in ld else None,
                        **ld['details'],
                        sqs_data=ld['question'],
                    )
                )
        except Exception as e:
            traceback.print_exc()
            logger.error(f'Error: {str(e)}')

    def _update_assessor_info(self, item, sqs_dict):
        assessor_info = sqs_dict.get('assessor_info')
        if assessor_info:
            self.add_info_assessor[item['name']] = assessor_info




