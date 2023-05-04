from django.test import TestCase
#import unittest
from django.core.cache import cache
import requests

from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData
from sqs.utils.loader_utils import DbLayerProvider
#from tests.no_createdb_test_runner import NoCreateDbTestRunner

from sqs.utils.das_tests.assessor_answers import other_isnotnull_colnames as colnames
from sqs.utils.das_tests.assessor_answers import other_isnotnull_prefix as prefix
from sqs.utils.das_tests.assessor_answers import other_isnotnull_polygons as polygons
from sqs.utils.das_tests.assessor_answers import checkbox_lessthan as cb_assessor
from sqs.utils.das_tests.assessor_answers import select_lessthan as select_assessor
from sqs.utils.das_tests.assessor_answers import multiselect_lessthan as ms_assessor
from sqs.utils.das_tests.assessor_answers import radiobuttons_lessthan as rb_assessor

import logging
logger = logging.getLogger(__name__)
logging.disable(logging.CRITICAL)


#class SetupCddpAssessorAnswerTests(unittest.TestCase):
class SetupCddpAssessorAnswerTests(TestCase):
    '''
    From: https://www.programcreek.com/python/?code=nesdis%2Fdjongo%2Fdjongo-master%2Ftests%2Fdjango_tests%2Ftests%2Fv21%2Ftests%2Ftest_runner%2Ftests.py#
          https://stackoverflow.com/questions/5917587/django-unit-tests-without-a-db

    To run:
        All tests in class SetupDatabasesTests
        ./manage.py test tests.test_cddp_assessor_answers.SetupCddpAssessorAnswerTests
 
        Specific test
        ./manage.py test tests.test_cddp_assessor_answers.SetupCddpAssessorAnswerTests.test_other_isnotnull_colname_in_answer

        Specific test, without caching
        ./manage.py test tests.test_cddp_assessor_answers.SetupCddpAssessorAnswerTests.test_other_isnotnull_colname_in_answer --disable-cache
    '''

#     def setUp(self):
#        # runs (repeats) for every test below
#        # self.runner_instance = NoCreateDbTestRunner(verbosity=0)
#        pass

#    @classmethod
#    def setUpTestData(cls):
#        pass

    @classmethod
    def setUpClass(self):
        # runs once for every test below
        #import ipdb; ipdb.set_trace()
        cache.clear()

        # create layer in test DB
        name='cddp:local_gov_authority'
        url='https://kmi.dbca.wa.gov.au/geoserver/dummy'
        filename='sqs/utils/das_tests/layers/cddp_local_gov_authority.json'
        layer_info, layer_gdf = DbLayerProvider(layer_name=name, url=url).get_layer_from_file(filename)

        # create layer in test DB
        name='cddp:dpaw_regions'
        url='https://kmi.dbca.wa.gov.au/geoserver/dummy'
        filename='sqs/utils/das_tests/layers/cddp_dpaw_regions.json'
        layer_info, layer_gdf = DbLayerProvider(layer_name=name, url=url).get_layer_from_file(filename)


    @classmethod
    def tearDownClass(self):
        cache.clear()

    def test_other_isnotnull_colname_in_answer(self):
        ''' tests masterlist questions where:
            1. proponent_answer column_name == column_name
            2. proponent_answer column_name != column_name
            3. proponent_answer --> static plain text
         '''
        logger.info("Method: test_other_isnotnull_colname_in_answer.")
        self.dlq = DisturbanceLayerQuery(colnames.MASTERLIST_QUESTIONS_GBQ, colnames.GEOJSON, colnames.PROPOSAL)
        res = self.dlq.query()
        #import ipdb; ipdb.set_trace()
        self.assertTrue(res['add_info_assessor'] == colnames.TEST_RESPONSE['add_info_assessor'])

    def test_other_isnotnull_prefix_in_answer(self):
        ''' tests masterlist questions where:
            1. proponent_answer with prefix
            2. proponent_answer without prefix
            3. proponent_answer with prefix and static answer string
         '''
        logger.info("Method: test_other_isnotnull_prefix_in_answer.")
        self.dlq = DisturbanceLayerQuery(prefix.MASTERLIST_QUESTIONS_GBQ, prefix.GEOJSON, prefix.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['add_info_assessor'] == prefix.TEST_RESPONSE['add_info_assessor'])

    def test_other_isnotnull_no_of_polygons(self):
        ''' tests masterlist questions where:
            1. proponent_answer with all polygons from intersection (-1)
            2. proponent_answer with 1 polygon from intersection (1)
            3. proponent_answer with with no result returned to proponent from polygons intersected 
         '''
        logger.info("Method: test_other_isnotnull_no_of_polygons.")
        self.dlq = DisturbanceLayerQuery(polygons.MASTERLIST_QUESTIONS_GBQ, polygons.GEOJSON, polygons.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['add_info_assessor'] == polygons.TEST_RESPONSE['add_info_assessor'])

    def test_checkbox_lessthan(self):
        logger.info("Method: test_checkbox_lessthan.")
        self.dlq = DisturbanceLayerQuery(cb_assessor.MASTERLIST_QUESTIONS_GBQ, cb_assessor.GEOJSON, cb_assessor.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['add_info_assessor'] == cb_assessor.TEST_RESPONSE['add_info_assessor'])

    def test_select_lessthan(self):
        logger.info("Method: test_select_lessthan.")
        self.dlq = DisturbanceLayerQuery(select_assessor.MASTERLIST_QUESTIONS_GBQ, select_assessor.GEOJSON, select_assessor.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['add_info_assessor'] == select_assessor.TEST_RESPONSE['add_info_assessor'])

    def test_multiselect_lessthan(self):
        logger.info("Method: test_multiselect_lessthan.")
        self.dlq = DisturbanceLayerQuery(ms_assessor.MASTERLIST_QUESTIONS_GBQ, ms_assessor.GEOJSON, ms_assessor.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['add_info_assessor'] == ms_assessor.TEST_RESPONSE['add_info_assessor'])

    def test_radiobuttons_lessthan(self):
        logger.info("Method: test_select_lessthan.")
        self.dlq = DisturbanceLayerQuery(rb_assessor.MASTERLIST_QUESTIONS_GBQ, rb_assessor.GEOJSON, rb_assessor.PROPOSAL)
        res = self.dlq.query()
        #import ipdb; ipdb.set_trace()
        self.assertTrue(res['add_info_assessor'] == rb_assessor.TEST_RESPONSE['add_info_assessor'])

