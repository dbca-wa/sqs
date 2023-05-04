from django.test import TestCase
#import unittest
from django.core.cache import cache

import requests

from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData
from tests.no_createdb_test_runner import NoCreateDbTestRunner
from sqs.utils.loader_utils import DbLayerProvider

from sqs.utils.das_tests.isnotnull import select_isnotnull as select
from sqs.utils.das_tests.isnotnull import multiselect_isnotnull as ms
from sqs.utils.das_tests.isnotnull import radiobuttons_isnotnull as rb
from sqs.utils.das_tests.isnotnull import checkbox_isnotnull as cb
from sqs.utils.das_tests.isnotnull import other_isnotnull as other
from sqs.utils.das_tests.isnotnull import other_isnotnull_visible as other_visible

import logging
logger = logging.getLogger(__name__)
logging.disable(logging.CRITICAL)


#class SetupCddpIsNotNullTests(unittest.TestCase):
class SetupCddpIsNotNullTests(TestCase):
    '''
    From: https://www.programcreek.com/python/?code=nesdis%2Fdjongo%2Fdjongo-master%2Ftests%2Fdjango_tests%2Ftests%2Fv21%2Ftests%2Ftest_runner%2Ftests.py#
          https://stackoverflow.com/questions/5917587/django-unit-tests-without-a-db

    To run:
        All tests in class SetupDatabasesTests
        ./manage.py test tests.test_cddp_isnotnull.SetupCddpIsNotNullTests
 
        Specific test
        ./manage.py test tests.test_cddp_isnotnull.SetupCddpIsNotNullTests.test_select_isnotnull

        Specific test, without caching
        ./manage.py test tests.test_cddp_isnotnull.SetupCddpIsNotNullTests.test_select_isnotnull --disable-cache
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

    def test_select_isnotnull(self):
        logger.info("Method: test_select_isnotnull.")
        self.dlq = DisturbanceLayerQuery(select.MASTERLIST_QUESTIONS_GBQ, select.GEOJSON, select.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['data'] == select.TEST_RESPONSE['data'])

    def test_multiselect_isnotnull(self):
        logger.info("Method: test_multi-select_isnotnull.")
        self.dlq = DisturbanceLayerQuery(ms.MASTERLIST_QUESTIONS_GBQ, ms.GEOJSON, ms.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['data'] == ms.TEST_RESPONSE['data'])

    def test_radiobuttons_isnotnull(self):
        logger.info("Method: test_radiobuttons_isnotnull.")
        self.dlq = DisturbanceLayerQuery(rb.MASTERLIST_QUESTIONS_GBQ, rb.GEOJSON, rb.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['data'] == rb.TEST_RESPONSE['data'])

    def test_checkbox_isnotnull(self):
        logger.info("Method: test_checkbox_isnotnull.")
        self.dlq = DisturbanceLayerQuery(cb.MASTERLIST_QUESTIONS_GBQ, cb.GEOJSON, cb.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['data'] == cb.TEST_RESPONSE['data'])

    def test_other_isnotnull(self):
        logger.info("Method: test_other_isnotnull.")
        self.dlq = DisturbanceLayerQuery(other.MASTERLIST_QUESTIONS_GBQ, other.GEOJSON, other.PROPOSAL)
        res = self.dlq.query()
        self.assertTrue(res['data'] == other.TEST_RESPONSE['data'])

    def test_other_isnotnull_visible(self):
        logger.info("Method: test_other_isnotnull_visible.")
        self.dlq = DisturbanceLayerQuery(other_visible.MASTERLIST_QUESTIONS_GBQ, other_visible.GEOJSON, other_visible.PROPOSAL)
        res = self.dlq.query()
        #import ipdb; ipdb.set_trace()
        self.assertTrue(res['data'] == other_visible.TEST_RESPONSE['data'])


