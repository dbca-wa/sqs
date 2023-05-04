import traceback
import os
import json

from sqs.utils.geoquery_utils import DisturbanceLayerQueryHelper
#from sqs.utils.helper  import SchemaSearch

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
        #self.flat_orig_data = SchemaSearch(orig_data).get_flat_dict()

        self.data = {}
        #self.data = layer_query_helper.proposal.get('data')[0]
        self.layer_data = []
        self.add_info_assessor = {}

    def prefill_data_from_shape(self):
        #data = {}

        schema = self.layer_query_helper.proposal.get('schema')

        try:
            for item in schema:
                #import ipdb; ipdb.set_trace()
                self.data.update(self._populate_data_from_item(item, 0, ''))
               
        except:
            traceback.print_exc()
        return [self.data]

    def _populate_data_from_item(self, item, repetition, suffix, sqs_value=None):

        item_data = {}

        if 'name' in item:
            extended_item_name = item['name']
        else:
            raise Exception('Missing name in item %s' % item['label'])

        if 'children' not in item:
            if item['type'] =='checkbox':
                #import ipdb; ipdb.set_trace()
                if sqs_value:
                    #import ipdb; ipdb.set_trace()
                    for val in sqs_value:
                        if val==item['label']:
                            item_data[item['name']]='on'

            elif item['type'] == 'file':
                #print('file item', item)
                pass
            else:
                    if item['type'] == 'multi-select':
                        #Get value from SQS. Value should be an array of the correct options.
                        #sqs_value=item['options'][1]['value']
                        #sqs_value=[sqs_value]
                        
                        # don't overwrite if propsal['data'] already has a value set
                        #sqs_values = self.flat_orig_data.get(item['name'])
                        #import ipdb; ipdb.set_trace()
                        sqs_dict = self.layer_query_helper.find_multiselect(item)
                        sqs_values = sqs_dict.get('result')
                        assessor_info = sqs_dict.get('assessor_info')
                        layer_details = sqs_dict.get('layer_details')
                        
                        if sqs_values:
                            self.add_info_assessor[item['name']] = assessor_info
                            self._update_layer_info(layer_details)

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
                        #sqs_value=item['options'][1]['value']
                        if item['type'] == 'select':
                            sqs_dict = self.layer_query_helper.find_select(item)
                            #import ipdb; ipdb.set_trace()
                        elif item['type'] == 'radiobuttons':
                            #import ipdb; ipdb.set_trace()
                            sqs_dict = self.layer_query_helper.find_radiobutton(item)

                        sqs_value = sqs_dict.get('result')
                        assessor_info = sqs_dict.get('assessor_info')
                        layer_details = sqs_dict.get('layer_details')
                        if sqs_value:
                            #self.add_info_assessor[item['name']] = sqs_value.get('assessor_answer')
                            self.add_info_assessor[item['name']] = assessor_info
                            self._update_layer_info(layer_details)

                            if item['options']:
                                for op in item['options']:
                                    #if sqs_value==op['value']:
                                    if sqs_value==op['label']:
                                        item_data[item['name']]=op['value']
                                        break

                    elif item['type'] in ['text', 'text_area']:
                        #All the other types e.g. text_area, text, date (except label).
                        #import ipdb; ipdb.set_trace()
                        if item['type'] != 'label':
                            sqs_dict = self.layer_query_helper.find_other(item)
                            #import ipdb; ipdb.set_trace()
                            #sqs_values = sqs_dict.get('result')
                            layer_details = sqs_dict.get('layer_details')
                            assessor_info = sqs_dict.get('assessor_info')

                            #if sqs_values:
                            if assessor_info:
                                item_data[item['name']] = assessor_info.get('proponent_answer')
                                self.add_info_assessor[item['name']] = assessor_info.get('assessor_answer')
                                self._update_layer_info(layer_details)
                    else:
                        #All the other types e.g. date, number etc (except label).
                        pass
        else:
            #import ipdb; ipdb.set_trace()
            #sqs_values = []
            if 'repetition' in item:
                item_data = self.generate_item_data_shape(extended_item_name,item,item_data,1,suffix)
            else:
                #item_data = generate_item_data_shape(extended_item_name, item, item_data,1,suffix)
                #Check if item has checkbox childer
                if self.check_checkbox_item(extended_item_name, item, item_data,1,suffix):
                    #make a call to sqs for item
                    # 1. question      --> item['label']
                    # 2. checkbox text --> item['children'][0]['label']
                    # 3. request response for all checkbox's ie. send item['children'][all]['label']. 
                    #    SQS will return a list of checkbox's answersfound eg. ['National park', 'Nature reserve']

                    sqs_dict = self.layer_query_helper.find_checkbox(item)
                    #import ipdb; ipdb.set_trace()
                    sqs_values = sqs_dict.get('result')
                    layer_details = sqs_dict.get('layer_details')
                    assessor_info = sqs_dict.get('assessor_info')
                    if sqs_values:
                        self.add_info_assessor[item['name']] = assessor_info
                        item_layer_data = self._update_layer_info(layer_details)
                        item_data = self.generate_item_data_shape(extended_item_name, item, item_data,1,suffix, sqs_values)
                else:
                    item_data = self.generate_item_data_shape(extended_item_name, item, item_data,1,suffix)


        if 'conditions' in item:
            try: 
                #if item['name'] == 'Section0-7': 
                #    import ipdb; ipdb.set_trace()
                #    pass
               
                #for condition in list(item['conditions'].keys()):
                for condition in item['conditions'].keys():
                    if item_data and condition==item_data[item['name']]:
                        for child in item['conditions'][condition]:
                            item_data.update(self._populate_data_from_item(child,  repetition, suffix))
            except Exception as e:
                import ipdb; ipdb.set_trace()
                pass

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
        #import ipdb; ipdb.set_trace()
        checkbox_item=False
        for child_item in item.get('children'):
            if child_item['type']=='checkbox':
                checkbox_item=True        
        return checkbox_item

    def _update_layer_info(self, layer_details):
        #import ipdb; ipdb.set_trace()
        layer_info = []
        for ld in layer_details:
            #layer_info.append(
            self.layer_data.append(
                dict(
                    name=ld['name'] if 'name' in ld else None,
                    label=ld['label'] if 'label' in ld else None,
                    **ld['details'],
                )
            )
        #return layer_info

#        return dict(
#            name=item_name,
#            layer_details=layer_details,
#            #new_layer_name='new layer name',
#            #new_layer_updated='new layer updated'
#        )


#def save_prefill_data(proposal):
#    prefill_instance= PrefillData()
#    try:
#        prefill_data = prefill_instance.prefill_data_from_shape(proposal.schema)
#        if prefill_data:
#            proposal.data=prefill_data
#            proposal.layer_data= prefill_instance.layer_data
#            print(prefill_instance.add_info_assessor)
#            proposal.add_info_assessor=prefill_instance.add_info_assessor
#            proposal.save()
#            return proposal
#    except:
#        raise



