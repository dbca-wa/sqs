from django.urls import reverse
from django.db import transaction
from django.test.runner import DiscoverRunner
from django.core.cache import cache
from rest_framework.test import APIClient
from rest_framework import status
#import unittest
from django.test import TestCase
import requests

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

        self.layer_upload_url = reverse('layers-list') + 'add_layer/' 
        self.spatial_query_url = reverse('das-list') + 'spatial_query/'


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
        self.data1 = dict(layer_name=self.layer_name1, url=url, geojson=geojson1)
        self.data2 = dict(layer_name=self.layer_name1, url=url, geojson=geojson2)

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

#        # create layer in test DB
#        name='public:dbca_trails_public'
#        url='https://kmi.dbca.wa.gov.au/geoserver/dummy'
#        filename='sqs/utils/das_tests/layers/public_dbca_trails_public.json'
#        layer_info, layer_gdf = DbLayerProvider(layer_name=name, url=url).get_layer_from_file(filename)
#
#        # create layer in test DB
#        name='public:dbca_districts_public'
#        url='https://kmi.dbca.wa.gov.au/geoserver/dummy'
#        filename='sqs/utils/das_tests/layers/public_dbca_districts_public.json'
#        layer_info, layer_gdf = DbLayerProvider(layer_name=name, url=url).get_layer_from_file(filename)

    @classmethod
    def tearDownClass(self):
        cache.clear()

    def test_layer_create_api(self):
        ''' POST create layer to SQS - http://localhost:8002/api/v1/das/add_layer/ 
        '''
        logger.info("Method: test_layer_create_api.")

        cache.clear()
        response = self.api_client.post(self.layer_upload_url, data=self.data1, format='json')

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        #self.assertIn('Created Layer', response.data)

    def test_layer_update_api(self):
        ''' POST new/update layer to SQS - http://localhost:8002/api/v1/das/add_layer/ 
            Returns 
        '''
        logger.info("Method: test_layer_update_api.")

        cache.clear()
        response1 = self.api_client.post(self.layer_upload_url, data=self.data1, format='json')
        response2 = self.api_client.post(self.layer_upload_url, data=self.data2, format='json')

        #self.assertIn('Layer updated', response2.data)
        self.assertEqual(response2.status_code, status.HTTP_200_OK)

    def test_layer_update_fail_api(self):
        ''' POST update layer to SQS. Should not update since layers have not changed - http://localhost:8002/api/v1/das/add_layer/ 
        '''
        logger.info("Method: test_layer_update_fail_api.")

        # try uploading the same layer twice - 2nd should fail to upload
        cache.clear()
        response1 = self.api_client.post(self.layer_upload_url, data=self.data1, format='json')
        response2 = self.api_client.post(self.layer_upload_url, data=self.data1, format='json')

        #import ipdb; ipdb.set_trace()
        self.assertIn('Layer not updated', response2.data)
        self.assertEqual(response2.status_code, status.HTTP_304_NOT_MODIFIED)

    def test_layer_version_dates_api(self):
        ''' POST new/update layer to SQS - http://localhost:8002/api/v1/das/add_layer/ 
            Returns 
        '''
        logger.info("Method: test_layer_version_dates_api.")

        cache.clear()
        response1 = self.api_client.post(self.layer_upload_url, data=self.data1, format='json')
        response2 = self.api_client.post(self.layer_upload_url, data=self.data2, format='json')
        num_dates = len(Layer.objects.get(name=self.layer_name1).get_obj_revision_dates())

        self.assertEquals(num_dates, 2)

    def test_layer_version_ids_api(self):
        ''' POST new/update layer to SQS - http://localhost:8002/api/v1/das/add_layer/ 
            Returns 
        '''
        logger.info("Method: test_layer_version_ids_api.")

        cache.clear()
        response1 = self.api_client.post(self.layer_upload_url, data=self.data1, format='json')
        response2 = self.api_client.post(self.layer_upload_url, data=self.data2, format='json')
        version_id = Layer.objects.get(name=self.layer_name1).get_obj_version_ids()[1]['version']

        self.assertEquals(version_id, 2)

    def test_layer_prev_revision_api(self):
        ''' POST new/update layer to SQS - http://localhost:8002/api/v1/das/add_layer/ 
            Returns 
        '''
        logger.info("Method: test_layer_prev_revision_api.")

        #import ipdb; ipdb.set_trace()
        cache.clear()
        response1 = self.api_client.post(self.layer_upload_url, data=self.data1, format='json')
        response2 = self.api_client.post(self.layer_upload_url, data=self.data2, format='json')
        obj_prev = Layer.objects.get(name=self.layer_name1).get_obj_revision_by_version(1)
        num_revisions = len(Layer.objects.get(name=self.layer_name1).get_obj_version_ids())

        self.assertEquals(obj_prev.version, num_revisions-1)

#
# Commented below test because Layer obj is created too slowly, and by the time layer_obj ../filter().exists() is called from DbLayerProvide, it still does not exist yet
#
#    def test_das_request_no_layer_exists_api(self):
#        ''' POST CDDP Request from DAS to SQS - http://localhost:8002/api/das/spatial_query/ 
#            Case where layers do not already exists on SQS:
#
#            Returns 
#                checks if layer specified in masterlist questions exists, if not the layers is fetched and saved on SQS.
#                Then results/answers of intersection of multi-polygon and these newly loaded layers is returned
#        '''
#        logger.info("Method: test_das_request_no_layer_exists_api.")
#
#        cache.clear()
#        response = self.api_client.post(self.spatial_query_url, data=CDDP_REQUEST_NO_LAYER_JSON, format='json')
#        import ipdb; ipdb.set_trace()
#        layer_district = response.data['layer_data'][0]['attrs'][0]['district']
#        self.assertIn('GOLDFIELDS REGION', layer_district)

#    def test_das_request_incorrect_layer_attrs_api(self):
#        ''' POST CDDP Request from DAS to SQS - http://localhost:8002/api/das/spatial_query/ 
#            Case where layers do not already exists on SQS, and incorrect layer attrs are specified in masterlist qusestions:
#
#            Returns 
#                checks if layer specified in masterlist questions exists, if not the layers is fetched and saved on SQS.
#                Then results/answers of intersection of multi-polygon and these newly loaded layers is returned
#        '''
#        logger.info("Method: test_das_request_incorrect_layer_attrs_api.")
#
#        cache.clear()
#        response = self.api_client.post(self.spatial_query_url, data=CDDP_REQUEST_INCORRECT_LAYER_ATTRS_JSON, format='json')
#        #import ipdb; ipdb.set_trace()
#        layer_details = response.data['layer_data'][0]
#        self.assertIn('not found in layer', layer_details['error_msg'])


