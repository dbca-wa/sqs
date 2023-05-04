from django.test import TestCase
#from django.test.simple import DjangoTestSuiteRunner
from django.test.runner import DiscoverRunner
from tests.no_db_test_runner import NoDbTestRunner

from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData
from sqs.utils.das_tests.select import GEOJSON, PROPOSAL
import requests

import logging
logger = logging.getLogger(__name__)

MASTERLIST_QUESTIONS_GBQ = [
  {
    "question_group": "1.0 In which something is this proposal located (Select Component)?",
    "questions": [
      {
        "id": 43,
        "question": "1.0 In which something is this proposal located (Select Component)?",
        "answer_mlq": "",
        "layer_name": "cddp:local_gov_authority",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:local_gov_authority&maxFeatures=200&outputFormat=application%2Fjson",
        "expiry": "2024-01-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "lga_label",
        "operator": "IsNotNull",
        "value": "",
        "prefix_answer": "",
        "no_polygons_proponent": -1,
        "answer": "",
        "prefix_info": "",
        "no_polygons_assessor": -1,
        "assessor_info": "",
        "regions": "All"
      }
    ]
  }
]

TEST_RESPONSE = {
 'system': 'DAS',
 'data': [{'proposalSelectSection': [{'Section1-0': 'CITY-OF-KALGOORLIE-BOULDER'}]}],
 'layer_data': [{'name': 'Section1-0',
   'label': None,
   'layer_name': 'cddp:local_gov_authority',
   'layer_created': '2022-05-17 07:28:48',
   'layer_version': 1,
   'sqs_timestamp': '2023-03-07 17:20:02'},
  {'name': 'Section1-0',
   'label': None,
   'layer_name': 'cddp:local_gov_authority',
   'layer_created': '2022-05-17 07:28:48',
   'layer_version': 1,
   'sqs_timestamp': '2023-03-07 17:21:09'},
  {'name': 'Section1-0',
   'label': None,
   'layer_name': 'cddp:local_gov_authority',
   'layer_created': '2022-05-17 07:28:48',
   'layer_version': 1,
   'sqs_timestamp': '2023-03-07 17:26:01'}],
 'add_info_assessor': {}
}




class SelectTestClass(TestCase):
    '''
    To run:
        ./manage.py test --testrunner=tests.no_db_test_runner.NoDbTestRunner --settings='sqs.settings_no_db'

        OR (since testrunner is declared in sqs/settings_no_db.py)

        ./manage.py test --settings='sqs.settings_no_db'
        python -m ipdb -c cont myscript.py
    '''

    @classmethod
    def setUpTestData(self):
        logger.info("setUpTestData: Run once to set up non-modified data for all class methods.")
        #import ipdb; ipdb.set_trace()
        #self.runner_instance = NoDbTestRunner(verbosity=1, settings='sqs.settings_no_db') 
        #self.runner_instance = DiscoverRunner(verbosity=1, testrunner='tests.no_db_test_runner.NoDbTestRunner', settings='sqs.settings_no_db') 
        pass

    def setUp(self):
        logger.info("setUp: Run once for every test method to setup clean data.")
        pass

#    def test_dotted_test_module(self):
#        logger.info("Method: test_dotted_test_module.")
#        count = DiscoverRunner().build_suite(
#            #['test_runner_apps.sample.tests_sample'],
#            ['tests'],
#        ).countTestCases()
#
#        self.assertEqual(count, 4)

    def test_select_isnull(self):
        logger.info("Method: test_select_isnull.")
        self.dlq = DisturbanceLayerQuery(MASTERLIST_QUESTIONS_GBQ, GEOJSON, PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['data'] == TEST_RESPONSE['data'])



