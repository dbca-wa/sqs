from django.test import TestCase
#import unittest
from django.core.cache import cache
import requests

from sqs.utils.das_schema_utils import DisturbanceLayerQuery, DisturbancePrefillData
from sqs.utils.loader_utils import DbLayerProvider
from sqs.components.gisquery.models import LayerRequestLog
#from tests.no_createdb_test_runner import NoCreateDbTestRunner

from sqs.utils.das_tests.request_log.das_query import DAS_QUERY_JSON

import logging
logger = logging.getLogger(__name__)
logging.disable(logging.CRITICAL)


class SetupRequestLogTests(TestCase):
    '''
    From: https://www.programcreek.com/python/?code=nesdis%2Fdjongo%2Fdjongo-master%2Ftests%2Fdjango_tests%2Ftests%2Fv21%2Ftests%2Ftest_runner%2Ftests.py#
          https://stackoverflow.com/questions/5917587/django-unit-tests-without-a-db

    To run:
        All tests in class SetupRequestLogTests
        ./manage.py test tests.test_request_log.SetupRequestLogTests
 
        Specific test
        ./manage.py test tests.test_request_log.SetupRequestLogTests.test_create_log

        Specific test, without caching
        ./manage.py test tests.test_request_log.SetupRequestLogTests.test_create_log --disable-cache
    '''

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
        
        self.request_log = LayerRequestLog.create_log(DAS_QUERY_JSON)
        self.masterlist_questions = DAS_QUERY_JSON['masterlist_questions']
        self.geojson = DAS_QUERY_JSON['geojson']
        self.proposal = DAS_QUERY_JSON['proposal']


    @classmethod
    def tearDownClass(self):
        cache.clear()

    def test_create_log(self):
        ''' tests the creation of the LayerRequestLog'''
        logger.info("Method: test_create_log.")
        self.dlq = DisturbanceLayerQuery(self.masterlist_questions, self.geojson, self.proposal)
        res = self.dlq.query()
        self.assertTrue(self.request_log.data['proposal']['system'] == 'DAS')

    def test_response(self):
        ''' tests the LayerRequestLog, following save of response to the instance '''
        logger.info("Method: test_response.")
        self.dlq = DisturbanceLayerQuery(self.masterlist_questions, self.geojson, self.proposal)
        res = self.dlq.query()

        self.request_log.response = res
        self.request_log.save()

        self.assertTrue(self.request_log.response['system'] == 'DAS')

    def test_request_details(self):
        ''' tests the LayerRequestLog request_details function, following save of response to the instance '''
        logger.info("Method: test_request_history.")
        self.dlq = DisturbanceLayerQuery(self.masterlist_questions, self.geojson, self.proposal)
        res = self.dlq.query()

        self.request_log.response = res
        self.request_log.save()

        history = self.request_log.request_details('DAS', app_id=1388)

        #import ipdb; ipdb.set_trace()
        self.assertTrue(history['num_layers_in_request'] == 2)


