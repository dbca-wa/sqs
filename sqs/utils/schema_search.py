from collections import OrderedDict
import json

import logging
logger = logging.getLogger(__name__)



class SchemaSearch():
    def __init__(self, dictionary):
        self.dictionary = dictionary

    def search(self, search_list):
        """
        To run:
            from sqs.utils.helper import search
            search(p.data, ['BRM', 'JM 1'])
            search(p.data, search_list=['finalSection.Section8-0', 'tenureSection.Section1-0.Section1-0-3'])
                output:
                    {'finalSection.Section8-0': 'yes',
                     'tenureSection.Section1-0.Section1-0-3': 'on'}
        """
        result = {}
        flat_dict = self._flatten(self.dictionary)
        for k, v in flat_dict.items():
            #import ipdb; ipdb.set_trace()
            for search_item in search_list:
                if k.lower()==search_item.lower():
                    result.update( {k: v} )

        return result

    def get_flat_dict(self):
        """
        To test/run:
            proposal = {'id': 1503, 'schema': [{'name': 'proposalSummarySection', 'type': 'section', 'label': '1. Proposal Summary', 'children': [{'name': 'Section0-0', 'type': 'text', 'label': '1.0 Proposal title', 'isRequired': 'true', 'help_text_url': 'site_url:/help/disturbance/user/anchor=Section0-0', '_help_text_assessor_url': 'site_url:/help/disturbance/assessor/anchor=Section0-0', 'isTitleColumnForDashboard': True}]}, {'name': 'Section1-2', 'type': 'multi-select', 'label': '1.2 In which Local Government Authority (LGA) is this proposal located?', 'options': [{'label': 'Nungarin', 'value': 'Nungarin', 'isRequired': 'true'}, {'label': 'Ngaanyatjarraku', 'value': 'Ngaanyatjarraku'}, {'label': 'Beverley', 'value': 'Beverley'}, {'label': 'Pingelly', 'value': 'Pingelly'}, {'label': 'Wongan-Ballidu', 'value': 'WonganBallidu'}], 'help_text_url': 'site_url:/help/disturbance/user/anchor=Section1-2'}, {'name': 'Section10-2', 'type': 'select', 'label': '10.2 In which something is this proposal located?', 'options': [{'label': 'Nungarin', 'value': 'Nungarin', 'isRequired': 'true'}, {'label': 'Ngaanyatjarraku', 'value': 'Ngaanyatjarraku'}, {'label': 'Beverley', 'value': 'Beverley'}, {'label': 'Pingelly', 'value': 'Pingelly'}, {'label': 'Wongan-Ballidu', 'value': 'WonganBallidu'}], 'help_text_url': 'site_url:/help/disturbance/user/anchor=Section10-2'}, {'name': 'checkboxSection', 'type': 'section', 'label': '2. Checkboxes', 'children': [{'name': 'Section11-0', 'type': 'group', 'label': '2.0 What is the land tenure or classification?', 'children': [{'name': 'Section11-0-1', 'type': 'checkbox', 'group': 'Section11-0', 'label': 'National park', 'isRequired': 'true'}, {'name': 'Section11-0-2', 'type': 'checkbox', 'group': 'Section11-0', 'label': 'Nature reserve'}, {'name': 'Section11-0-3', 'type': 'checkbox', 'group': 'Section11-0', 'label': 'Conservation park'}, {'name': 'Section11-0-4', 'type': 'checkbox', 'group': 'Section11-0', 'label': 'State forest'}], 'help_text_url': 'site_url:/help/disturbance/user/anchor=Section11-5'}]}, {'name': 'radioSection', 'type': 'section', 'label': 'Radio Test', 'children': [{'name': 'Section12-0', 'type': 'radiobuttons', 'label': 'Radio first', 'options': [{'label': 'Yes', 'value': 'yes'}, {'label': 'No', 'value': 'no'}], 'conditions': {'yes': [{'name': 'Section12-0-YesGroup', 'type': 'group', 'label': '', 'children': [{'name': 'Section12-0-Yes1', 'type': 'radiobuttons', 'label': 'Radio 2', 'options': [{'label': 'One option', 'value': 'oneoption'}, {'label': 'two option', 'value': 'twooption'}]}]}]}}]}], 'data': [{'Section1-2': ['Nungarin', 'Beverley'], 'Section10-2': 'Pingelly', 'radioSection': [{'Section12-0': 'yes', 'Section12-0-YesGroup': [{'Section12-0-Yes1': 'twooption'}]}], 'checkboxSection': [{'Section11-0': [{'Section11-0-1': 'on', 'Section11-0-2': 'on'}]}], 'proposalSummarySection': [{'Section0-0': 'nan. GOLDFIELDS'}]}], 'layer_data': None, 'add_info_assessor': None}

            from sqs.utils.helper import get_flat_dict
            get_flat_dict(proposal['data'])
                output:
                    {'Section0-0': 'nan. GOLDFIELDS',
                     'Section1-0': None,
                     'Section1-2': ['Nungarin', 'Beverley'],
                     'Section10-2': 'Pingelly',
                     'Section11-0-1': 'on',
                     'Section11-0-2': 'on',
                     'Section12-0': 'yes',
                     'Section12-0-Yes1': 'twooption'}
        """
        result = {}
        flat_dict = self._flatten(self.dictionary)
        for k, v in flat_dict.items():
            #import ipdb; ipdb.set_trace()
            key = k.split('.')[-1]
            result.update( {key: v} )

        return result



    def _flatten(self, old_data,new_data=None, parent_key='', sep='.', width=4):
        '''
        Json-style nested dictionary / list flattener
        :old_data: the original data
        :new_data: the result dictionary
        :parent_key: all keys will have this prefix
        :sep: the separator between the keys
        :width: width of the field when converting list indexes
        '''

        def is_json(_str):
            ''' checks if _str is valid JSON '''
            try:
                json.loads(_str)
            except ValueError as e:
                return False
            except TypeError as e:
                return False
            return True

        if new_data is None:
            #new_data = {}
            new_data = OrderedDict()

        if isinstance(old_data, dict):
            for k, v in old_data.items():
                new_key = parent_key + sep + k if parent_key else k
                self._flatten(v, new_data, new_key, sep, width)
        elif isinstance(old_data, list):
            if len(old_data) == 1:
                self._flatten(old_data[0], new_data, parent_key, sep, width)
            else:
                self._flatten(json.dumps(old_data), new_data, parent_key, sep, width)
                #_flatten(','.join(old_data), new_data, parent_key, sep, width)
                #for i, elem in enumerate(old_dta):
                #    new_key = "{}{}{:0>{width}}".format(parent_key, sep if parent_key else '', i, width=width)
                #    _flatten(elem, new_data, new_key, sep, width)
        else:
            if parent_key not in new_data:
                #import ipdb; ipdb.set_trace()
                new_data[parent_key] = old_data if not is_json(old_data) else json.loads(old_data)
                #new_data[parent_key.split('.')[-1]] = old_data
            else:
                raise AttributeError("key {} is already used".format(parent_key))

        return new_data







