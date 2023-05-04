import unittest
import requests

from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData
from tests.no_db_test_runner import NoDbTestRunner

from sqs.utils.das_tests.select import select_isnotnull as select
from sqs.utils.das_tests.select import multiselect_isnotnull as ms
from sqs.utils.das_tests.select import radiobuttons_isnotnull as rb
from sqs.utils.das_tests.select import checkbox_isnotnull as cb
from sqs.utils.das_tests.select import other_isnotnull as other

import logging
logger = logging.getLogger(__name__)


class SetupDatabasesTests(unittest.TestCase):
    '''
    From: https://www.programcreek.com/python/?code=nesdis%2Fdjongo%2Fdjongo-master%2Ftests%2Fdjango_tests%2Ftests%2Fv21%2Ftests%2Ftest_runner%2Ftests.py#
          https://stackoverflow.com/questions/5917587/django-unit-tests-without-a-db

    To run:
        All tests in class SetupDatabasesTests
        ./manage.py test tests.discover_runner_test.SetupDatabasesTests
 
        Specific test
        ./manage.py test tests.discover_runner_test.SetupDatabasesTests.test_select_isnotnull
    '''

    def setUp(self):
        #self.runner_instance = DiscoverRunner(verbosity=3)
        self.runner_instance = NoDbTestRunner(verbosity=0)

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
        #import ipdb; ipdb.set_trace()
        self.assertTrue(res['data'] == other.TEST_RESPONSE['data'])


