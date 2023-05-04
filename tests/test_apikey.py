from django.urls import reverse
from django.core.cache import cache
from rest_framework.test import APIClient
from rest_framework import status
#import unittest
from django.test import TestCase
import requests

#from tests.no_createdb_test_runner import NoCreateDbTestRunner
from sqs.utils.das_tests.api.cddp_request import CDDP_REQUEST_JSON
from sqs.utils.das_tests.api.cddp_request_single import CDDP_REQUEST_SINGLE_JSON, TEST_RESPONSE
from sqs.utils.das_tests.request_log.das_query import DAS_QUERY_JSON
from sqs.utils.loader_utils import DbLayerProvider
from sqs.components.api.models import API

import logging
logger = logging.getLogger(__name__)
logging.disable(logging.CRITICAL)

#class SetupApiTests(unittest.TestCase):
class SetupApiKeyTests(TestCase):
    '''
    From: https://www.programcreek.com/python/?code=nesdis%2Fdjongo%2Fdjongo-master%2Ftests%2Fdjango_tests%2Ftests%2Fv21%2Ftests%2Ftest_runner%2Ftests.py#
          https://stackoverflow.com/questions/5917587/django-unit-tests-without-a-db

    To run:
        All tests in class SetupDatabasesTests
        ./manage.py test tests.test_apikey.SetupApiKeyTests
 
        Specific test
        ./manage.py test tests.test_apikey.SetupApiKeyTests.test_das_request_url
    '''

#    def setUp(self):
         # runs (repeats) for every test below
#        pass

#    @classmethod
#    def setUpTestData(cls):
#        pass

    @classmethod
    def setUpClass(self):
        # runs once for every test below
        #import ipdb; ipdb.set_trace()
        cache.clear()

        self.api_client = APIClient()

        apikey = 'DUMMY_APIKEY'
        self.api_obj = API.objects.create(system_name='DAS-UnitTest', system_id='S123', api_key=apikey, allowed_ips='127.0.0.1/32', active=True)


        self.point_query_url = reverse('layers-list') + 'point_query/'
        self.spatial_query_url = reverse('das-list') + 'spatial_query/'
        self.sqs_layer_query_url = reverse('layers-list') #+ '50/layer.json/'
        self.kmi_geojson_query_url = 'https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:dbca_regions&maxFeatures=50&outputFormat=application%2Fjson'

        apikey2 = 'DUMMY_APIKEY_2'
        self.api_obj = API.objects.create(system_name='DAS-UnitTest_2', system_id='S124', api_key=apikey2, allowed_ips='128.0.0.1/32', active=True)
        self.spatial_query_url2 = reverse('das-list') + 'spatial_query/'

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

    def test_das_request_single_correct_apikey_url(self):
        ''' POST CDDP Request from DAS to SQS for a single question - correct APIKEY
            Returns results/answers of intersection of multi-polygon and layers
        '''
        logger.info("Method: test_das_request_single_apikey_url.")

        response = self.api_client.post(self.spatial_query_url, data=CDDP_REQUEST_SINGLE_JSON, format='json')
        self.assertEqual(response.data['data'], TEST_RESPONSE['data'])

#    def test_das_request_single_incorrect_apikey_url(self):
#        ''' POST CDDP Request from DAS to SQS for a single question - incorrect APIKEY
#            Returns results/answers of intersection of multi-polygon and layers
#        '''
#        logger.info("Method: test_das_request_single_incorrect_apikey_url.")
#
#        spatial_query_url = reverse('das-list') + 'spatial_query/'
#        response = self.api_client.post(spatial_query_url, data=CDDP_REQUEST_SINGLE_JSON, format='json')
#        self.assertEqual(response.data['message'], 'API Key Not Found')

    def test_das_request_single_unknown_ip_address_url(self):
        ''' POST CDDP Request from DAS to SQS for a single question - Unknown IP Address
            Returns results/answers of intersection of multi-polygon and layers
        '''
        logger.info("Method: test_das_request_single_unknown_ip_address_url.")

        response = self.api_client.post(self.spatial_query_url2, data=CDDP_REQUEST_SINGLE_JSON, format='json')
        self.assertEqual(response.data['message'], 'Error')

    def test_das_request_single_throw_exception_url(self):
        ''' POST CDDP Request from DAS to SQS for a single question - throw Exception (testing the decorator functionality)
            Returns results/answers of intersection of multi-polygon and layers
        '''
        logger.info("Method: test_das_request_single_throw_exception_url.")

        response = self.api_client.post(self.spatial_query_url, data=CDDP_REQUEST_SINGLE_JSON.pop('proposal'), format='json')
        #import ipdb; ipdb.set_trace()
        self.assertEqual(response.data['message'], 'Error')




