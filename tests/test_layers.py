from django.urls import reverse
from django.db import transaction
from django.test.runner import DiscoverRunner
from django.core.cache import cache
from rest_framework.test import APIClient
from rest_framework import status
#import unittest
from django.test import TestCase
import requests
import json

from tests.no_createdb_test_runner import NoCreateDbTestRunner
import geopandas as gpd

from sqs.utils.loader_utils import LayerLoader
from sqs.utils.loader_utils import DbLayerProvider
from sqs.components.gisquery.models import Layer
from sqs.utils.das_tests.api.cddp_request_no_layer import CDDP_REQUEST_NO_LAYER_JSON
from sqs.utils.das_tests.api.cddp_request_incorrect_layer_attrs import CDDP_REQUEST_INCORRECT_LAYER_ATTRS_JSON
from sqs.components.api.models import API

import logging
logger = logging.getLogger(__name__)
logging.disable(logging.CRITICAL)


#class SetupLayerTests(unittest.TestCase):
class SetupLayerTests(TestCase):
    '''
    From: https://www.programcreek.com/python/?code=nesdis%2Fdjongo%2Fdjongo-master%2Ftests%2Fdjango_tests%2Ftests%2Fv21%2Ftests%2Ftest_runner%2Ftests.py#
          https://stackoverflow.com/questions/5917587/django-unit-tests-without-a-db

    To run:
        All tests
        ./manage.py test

        All tests in class SetupLayerTests
        ./manage.py test tests.test_layers.SetupLayerTests
 
        Specific test
        ./manage.py test tests.test_layers.SetupLayerTests.test_layer_prev_revision_api
    '''

#    def setUp(self):
         # runs (repeats) for every test below
#        pass

#    @classmethod
#    def setUpTestData(cls):
#        pass

    @classmethod
    def setUpClass(self):
        #self.runner_instance = DiscoverRunner(verbosity=1)
        #self.runner_instance = NoCreateDbTestRunner(verbosity=1)
        cache.clear()
        self.api_client = APIClient()

        apikey = 'DUMMY_APIKEY'
        api_obj = API.objects.create(system_name='DAS-UnitTest', system_id='S123', api_key=apikey, allowed_ips='127.0.0.1/32', active=True)

        #self.layer_upload_url = reverse('layers-list') + 'add_layer/' 
        #self.spatial_query_url = reverse('das-list') + 'spatial_query/'
        self.layer_upload_url = reverse('add_layer')
        self.spatial_query_url = reverse('das')


        #import ipdb; ipdb.set_trace()
        url = 'https://kmi.dbca.wa.gov.au/geoserver/cddp/dummy_test_url'

        # the following two geojson are sligtly different
#        filename1 = 'sqs/utils/das_tests/layers/dbca_regions_test1.json'
#        filename2 = 'sqs/utils/das_tests/layers/dbca_regions_test2.json'
        filename1 = 'sqs/utils/das_tests/layers/cddp_dpaw_regions.json'
        filename2 = 'sqs/utils/das_tests/layers/cddp_local_gov_authority.json'

        geojson1 = LayerLoader.retrieve_layer_from_file(filename1)
        geojson2 = LayerLoader.retrieve_layer_from_file(filename2)
        self.layer_name1 = 'test_layer1'
        #self.data1 = dict(layer_name=self.layer_name1, url=url, geojson=geojson1)
        #self.data2 = dict(layer_name=self.layer_name1, url=url, geojson=geojson2)
        self.data1 = {'id': 1, 'layer_name': self.layer_name1, 'layer_url': url, 'available_on_sqs': True, 'active_on_sqs': True}

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

'''
The below commented because the test now required a geojson to be passed, representing the Geoserver layer response
Need to update the api endpoint to allow unit-testing
'''
#    def test_layer_create_api(self):
#        ''' POST create layer to SQS - http://localhost:8002/api/v1/add_layer/ 
#        '''
#        logger.info("Method: test_layer_create_api.")
#
#        cache.clear()
#        response = self.api_client.post(self.layer_upload_url, data={'layer_details': json.dumps(self.data1)})
#
#        self.assertIn('GET Request from SQS to Geoserver failed', str(response.content))
#        #self.assertEqual(response.status_code, status.HTTP_201_CREATED)
#
#    def test_layer_update_api(self):
#        ''' POST new/update layer to SQS - http://localhost:8002/api/v1/das/add_layer/ 
#            Returns 
#        '''
#        logger.info("Method: test_layer_update_api.")
#
#        cache.clear()
#        response1 = self.api_client.post(self.layer_upload_url, data=self.data1, format='json')
#        response2 = self.api_client.post(self.layer_upload_url, data=self.data2, format='json')
#
#        #self.assertIn('Layer updated', response2.data)
#        self.assertEqual(response2.status_code, status.HTTP_200_OK)
#
#    def test_layer_update_fail_api(self):
#        ''' POST update layer to SQS. Should not update since layers have not changed - http://localhost:8002/api/v1/das/add_layer/ 
#        '''
#        logger.info("Method: test_layer_update_fail_api.")
#
#        # try uploading the same layer twice - 2nd should fail to upload
#        cache.clear()
#        response1 = self.api_client.post(self.layer_upload_url, data=self.data1, format='json')
#        response2 = self.api_client.post(self.layer_upload_url, data=self.data1, format='json')
#
#        #import ipdb; ipdb.set_trace()
#        self.assertIn('Layer not updated', response2.data)
#        self.assertEqual(response2.status_code, status.HTTP_304_NOT_MODIFIED)
#
#    def test_layer_version_dates_api(self):
#        ''' POST new/update layer to SQS - http://localhost:8002/api/v1/das/add_layer/ 
#            Returns 
#        '''
#        logger.info("Method: test_layer_version_dates_api.")
#
#        cache.clear()
#        response1 = self.api_client.post(self.layer_upload_url, data=self.data1, format='json')
#        response2 = self.api_client.post(self.layer_upload_url, data=self.data2, format='json')
#        num_dates = len(Layer.objects.get(name=self.layer_name1).get_obj_revision_dates())
#
#        self.assertEquals(num_dates, 2)
#
#    def test_layer_version_ids_api(self):
#        ''' POST new/update layer to SQS - http://localhost:8002/api/v1/das/add_layer/ 
#            Returns 
#        '''
#        logger.info("Method: test_layer_version_ids_api.")
#
#        cache.clear()
#        response1 = self.api_client.post(self.layer_upload_url, data=self.data1, format='json')
#        response2 = self.api_client.post(self.layer_upload_url, data=self.data2, format='json')
#        version_id = Layer.objects.get(name=self.layer_name1).get_obj_version_ids()[1]['version']
#
#        self.assertEquals(version_id, 2)
#
#    def test_layer_prev_revision_api(self):
#        ''' POST new/update layer to SQS - http://localhost:8002/api/v1/das/add_layer/ 
#            Returns 
#        '''
#        logger.info("Method: test_layer_prev_revision_api.")
#
#        #import ipdb; ipdb.set_trace()
#        cache.clear()
#        response1 = self.api_client.post(self.layer_upload_url, data=self.data1, format='json')
#        response2 = self.api_client.post(self.layer_upload_url, data=self.data2, format='json')
#        obj_prev = Layer.objects.get(name=self.layer_name1).get_obj_revision_by_version(1)
#        num_revisions = len(Layer.objects.get(name=self.layer_name1).get_obj_version_ids())
#
#        self.assertEquals(obj_prev.version, num_revisions-1)


