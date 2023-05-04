from django.test import TestCase
#import unittest
from django.core.cache import cache

import requests

from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData
#from tests.no_createdb_test_runner import NoCreateDbTestRunner
from sqs.utils.loader_utils import DbLayerProvider

from sqs.utils.das_tests.proponent_answers import other_isnotnull_visible as visible
from sqs.utils.das_tests.proponent_answers import other_isnotnull_colnames as colnames
from sqs.utils.das_tests.proponent_answers import other_isnotnull_prefix as prefix
from sqs.utils.das_tests.proponent_answers import other_isnotnull_polygons as polygons

import logging
logger = logging.getLogger(__name__)
logging.disable(logging.CRITICAL)


#class SetupCddpProponentAnswerTests(unittest.TestCase):
class SetupCddpProponentAnswerTests(TestCase):
    '''
    From: https://www.programcreek.com/python/?code=nesdis%2Fdjongo%2Fdjongo-master%2Ftests%2Fdjango_tests%2Ftests%2Fv21%2Ftests%2Ftest_runner%2Ftests.py#
          https://stackoverflow.com/questions/5917587/django-unit-tests-without-a-db

    To run:
        All tests in class SetupDatabasesTests
        ./manage.py test tests.test_cddp_proponent_answers.SetupCddpProponentAnswerTests
 
        Specific test
        ./manage.py test tests.test_cddp_proponent_answers.SetupCddpProponentAnswerTests.test_other_isnotnull_visible

        Specific test, without caching
        ./manage.py test tests.test_cddp_proponent_answers.SetupCddpProponentAnswerTests.test_other_isnotnull_visible --disable-cache
    '''

    @classmethod
    def setUpClass(self):
        #self.runner_instance = DiscoverRunner(verbosity=3)
        #self.runner_instance = NoCreateDbTestRunner(verbosity=0)
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

    def test_other_isnotnull_visible(self):
        ''' tests masterlist questions where:
            1. visibility = True
            2. visibility = False
         '''
        logger.info("Method: test_other_isnotnull_visible.")
        self.dlq = DisturbanceLayerQuery(visible.MASTERLIST_QUESTIONS_GBQ, visible.GEOJSON, visible.PROPOSAL)
        res = self.dlq.query()
        #import ipdb; ipdb.set_trace()
        self.assertTrue(res['data'] == visible.TEST_RESPONSE['data'])

    def test_other_isnotnull_colname_in_answer(self):
        ''' tests masterlist questions where:
            1. proponent_answer column_name == column_name
            2. proponent_answer column_name != column_name
            3. proponent_answer --> static plain text
         '''
        logger.info("Method: test_other_isnotnull_colname_in_answer.")
        self.dlq = DisturbanceLayerQuery(colnames.MASTERLIST_QUESTIONS_GBQ, colnames.GEOJSON, colnames.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['data'] == colnames.TEST_RESPONSE['data'])

    def test_other_isnotnull_prefix_in_answer(self):
        ''' tests masterlist questions where:
            1. proponent_answer with prefix
            2. proponent_answer without prefix
            3. proponent_answer with prefix and static answer string
         '''
        logger.info("Method: test_other_isnotnull_prefix_in_answer.")
        self.dlq = DisturbanceLayerQuery(prefix.MASTERLIST_QUESTIONS_GBQ, prefix.GEOJSON, prefix.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['data'] == prefix.TEST_RESPONSE['data'])

    def test_other_isnotnull_no_of_polygons(self):
        ''' tests masterlist questions where:
            1. proponent_answer with all polygons from intersection (-1)
            2. proponent_answer with 1 polygon from intersection (1)
            3. proponent_answer with with no result returned to proponent from polygons intersected 
         '''
        logger.info("Method: test_other_isnotnull_no_of_polygons.")
        self.dlq = DisturbanceLayerQuery(polygons.MASTERLIST_QUESTIONS_GBQ, polygons.GEOJSON, polygons.PROPOSAL)
        res = self.dlq.query()
        #import ipdb; ipdb.set_trace()
        self.assertTrue(res['data'] == polygons.TEST_RESPONSE['data'])



