from django.test import TestCase
#import unittest
from django.core.cache import cache

import requests

from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData
#from tests.no_createdb_test_runner import NoCreateDbTestRunner
from sqs.utils.loader_utils import DbLayerProvider

from sqs.utils.das_tests.lessthan import select_lessthan as select
from sqs.utils.das_tests.lessthan import multiselect_lessthan as ms
from sqs.utils.das_tests.lessthan import radiobuttons_lessthan as rb
from sqs.utils.das_tests.lessthan import radiobuttons_default_lessthan as rb_default
from sqs.utils.das_tests.lessthan import radiobuttons_conditions as rb_conditions
from sqs.utils.das_tests.lessthan import checkbox_lessthan as cb
from sqs.utils.das_tests.lessthan import other_lessthan as other

import logging
logger = logging.getLogger(__name__)
logging.disable(logging.CRITICAL)


#class SetupCddpLessThanTests(unittest.TestCase):
class SetupCddpLessThanTests(TestCase):
    '''
    From: https://www.programcreek.com/python/?code=nesdis%2Fdjongo%2Fdjongo-master%2Ftests%2Fdjango_tests%2Ftests%2Fv21%2Ftests%2Ftest_runner%2Ftests.py#
          https://stackoverflow.com/questions/5917587/django-unit-tests-without-a-db

    To run:
        All tests in all files beginnig with 'tests_' in directory <base-dir>/tests
        ./manage.py test

        All tests in class SetupCddpEqualsTests
        ./manage.py test tests.test_cddp_lessthan.SetupCddpLessThanTests
 
        Specific test
        ./manage.py test tests.test_cddp_lessthan.SetupCddpGreaterThanTests.test_select_lessthan

        Specific test, without caching
        ./manage.py test tests.test_cddp_lessthan.SetupCddpGreaterThanTests.test_select_lessthan --disable-cache
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

    def test_select_lessthan(self):
        logger.info("Method: test_select_lessthan.")
        self.dlq = DisturbanceLayerQuery(select.MASTERLIST_QUESTIONS_GBQ, select.GEOJSON, select.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['data'] == select.TEST_RESPONSE['data'])

    def test_multiselect_lessthan(self):
        logger.info("Method: test_multi-select_lessthan.")
        self.dlq = DisturbanceLayerQuery(ms.MASTERLIST_QUESTIONS_GBQ, ms.GEOJSON, ms.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['data'] == ms.TEST_RESPONSE['data'])

    def test_radiobuttons_lessthan(self):
        logger.info("Method: test_radiobuttons_lessthan.")
        self.dlq = DisturbanceLayerQuery(rb.MASTERLIST_QUESTIONS_GBQ, rb.GEOJSON, rb.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['data'] == rb.TEST_RESPONSE['data'])

    def test_radiobuttons_default_lessthan(self):
        logger.info("Method: test_radiobuttons_default_lessthan.")
        self.dlq = DisturbanceLayerQuery(rb_default.MASTERLIST_QUESTIONS_GBQ, rb_default.GEOJSON, rb_default.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['data'] == rb_default.TEST_RESPONSE['data'])

    def test_radiobuttons_conditions(self):
        logger.info("Method: test_radiobuttons_default_lessthan.")
        self.dlq = DisturbanceLayerQuery(rb_conditions.MASTERLIST_QUESTIONS_GBQ, rb_conditions.GEOJSON, rb_conditions.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['data'] == rb_conditions.TEST_RESPONSE['data'])

    def test_checkbox_lessthan(self):
        logger.info("Method: test_checkbox_lessthan.")
        self.dlq = DisturbanceLayerQuery(cb.MASTERLIST_QUESTIONS_GBQ, cb.GEOJSON, cb.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['data'] == cb.TEST_RESPONSE['data'])

    def test_other_lessthan(self):
        logger.info("Method: test_other_lessthan.")
        self.dlq = DisturbanceLayerQuery(other.MASTERLIST_QUESTIONS_GBQ, other.GEOJSON, other.PROPOSAL)
        res = self.dlq.query()
        #import ipdb; ipdb.set_trace()
        self.assertTrue(res['data'] == other.TEST_RESPONSE['data'])


