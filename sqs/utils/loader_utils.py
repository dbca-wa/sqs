from django.contrib.gis.geos import GEOSGeometry, Polygon, MultiPolygon
from django.conf import settings
from django.db import transaction
from django.core.cache import cache

from rest_framework.status import HTTP_200_OK, HTTP_201_CREATED, HTTP_202_ACCEPTED, HTTP_304_NOT_MODIFIED, HTTP_404_NOT_FOUND

import geopandas as gpd
import requests
import json
import os

from sqs.components.gisquery.models import Layer
from sqs.exceptions import LayerProviderException

import logging
logger = logging.getLogger(__name__)

DATETIME_FMT = '%Y-%m-%d %H:%M:%S'


class LayerLoader():
    """
    In [22]: from sqs.utils.loader_utils import LayerLoader
    In [23]: l=LayerLoader(url,name)
    In [24]: l.load_layer()


    In [23]: layer=Layer.objects.last()
    In [25]: layer.features.all().count()
    Out[25]: 9
    """

    def __init__(self, url='https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:dpaw_regions&maxFeatures=50&outputFormat=application%2Fjson', name='cddp:dpaw_regions'):
        self.url = url
        self.name = name
        self.data = ''
        
    def retrieve_layer(self):
        try:
            #import ipdb; ipdb.set_trace()
            if 'public' not in self.name:
                res = requests.get('{}'.format(self.url), auth=(settings.LEDGER_USER,settings.LEDGER_PASS), verify=None)
            else:
                res = requests.get('{}'.format(self.url), verify=None)

            res.raise_for_status()
            #cache.set('department_users',json.loads(res.content).get('objects'),10800)
            return res.json()
        except Exception as e:
            raise

    @classmethod
    def retrieve_layer_from_file(self, filename):
        try:
            with open(filename) as json_file:
                data = json.load(json_file)
            return data
        except Exception as e:
            raise


    def load_layer(self, filename=None, geojson=None):

        #layer_gdf = gpd.read_file('sqs/data/json/dpaw_regions.json')
        #layer_gdf = gpd.read_file(io.BytesIO(geojson_str))
        #import ipdb; ipdb.set_trace()

        try:
            layer = None
            if filename is not None:
                # get GeoJSON from file
                geojson = self.retrieve_layer_from_file(filename)
            elif geojson is None:
                # get GeoJSON from GeoServer
                geojson = self.retrieve_layer()

            layer_gdf1 = gpd.read_file(json.dumps(geojson))

            # uniformly projected layers in DB (allows buffer in meters by default)
            layer_gdf1.to_crs(settings.CRS, inplace=True) 

            qs_layer = Layer.objects.filter(name=self.name)
            with transaction.atomic():
                if len(qs_layer)==1:
                    # check if this layer already exists in DB. If it does exist, 
                    # check if there is a difference between existing layer and new layer from GeoServer.
                    # Only save new layer if its different.

                    #import ipdb; ipdb.set_trace()
                    layer = qs_layer[0]
                    if layer_is_unchanged(layer_gdf1, layer.to_gdf):
                        # no change in geojson
                        if layer.active == False:
                            # if not already active, set active
                            layer.active = True

                        if layer.url != self.url:
                            # url in masterlist_question may have been updated!
                            layer.url = True

                        self.data = dict(status=HTTP_304_NOT_MODIFIED, data=f'Layer not updated (no change to existing layer in DB): {self.name}')
                    else:
                        layer.url = self.url
                        layer.geojson = geojson
                        layer.active = True
                        self.data = dict(status=HTTP_200_OK, data=f'Layer updated: {self.name}')

                    layer.save()
                else:
                    # Layer does not exist in DB, so create
                    layer = Layer.objects.create(name=self.name, url=self.url, geojson=geojson)
                    self.data = dict(status=HTTP_201_CREATED, data=f'Layer created: {self.name}')

                logger.info(self.data)

        except Exception as e: 
            raise Exception(f'Error creating/updating layer from GeoServer.\n{str(e)}')
        
        return  layer

def layer_is_unchanged(gdf1, gdf2):
    #import ipdb; ipdb.set_trace()
    gdf1 = gdf1.reindex(sorted(gdf1.columns), axis=1)
    gdf2 = gdf2.reindex(sorted(gdf2.columns), axis=1)
    return gdf1.loc[:, ~gdf1.columns.isin(['id', 'md5_rowhash'])].equals(gdf2.loc[:, ~gdf2.columns.isin(['id', 'md5_rowhash'])])

#def has_layer_changed(layer_gdf1, layer_gdf2):
#
#    #import ipdb; ipdb.set_trace()
#    # check columns are the same
#    cols1 = list(layer_gdf1.columns.sort_values())
#    cols2 = list(layer_gdf2.columns.sort_values())
#    if cols1 != cols2:
#        # GeoJSON has changed
#        return True
#
#    # remove the 'id' column from layer_gdf's and sort the columns [index(axis=1)]
#    layer_gdf1 = layer_gdf1.loc[:, layer_gdf1.columns!='id'].sort_index(axis=1)
#    layer_gdf2 = layer_gdf2.loc[:, layer_gdf2.columns!='id'].sort_index(axis=1)
#
#    # check geo dataframes are the same
#    #if (layer_gdf1 == layer_gdf2).eq(True).all().eq(True).all():
#    if layer_gdf1.equals(layer_gdf2):
#        # GeoJSON has not changed
#        return False
#
#    return True

class DbLayerProvider():
    '''
    Utility class to return the requested layer.

        1. checks cache, if exists returns layer from cache
        2. checks DB, if exists returns layer from DB
        3. Layer not available in SQS:
            a. API Call to GeoServer
            b. Uploads layer geojson to SQS DB
            c. Updates cache with new layer

        Returns: layer_info, layer_gdf

    Usage:
        from sqs.utils.loader_utils import DbLayerProvider

        name='cddp:local_gov_authority'
        url='https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:local_gov_authority&maxFeatures=50&outputFormat=application%2Fjson'
        layer_info, layer_gdf = DbLayerProvider(layer_name, url=layer_url).get_layer()
    '''
    LAYER_CACHE = "LAYER_CACHE_{}"
    LAYER_DETAILS_CACHE = "LAYER_DETAILS_CACHE_{}"

    def __init__(self, layer_name, url):
        self.layer_name = layer_name
        self.url = url

    def get_layer(self):
        '''
        Returns: layer_info, layer_gdf
        '''
        try:
            #import ipdb; ipdb.set_trace()
            # try getting from cache
            layer_info, layer_gdf = self.get_from_cache()
            if layer_gdf is not None:
                logger.info(f'Getting layer from cache {self.layer_name} from:\n{self.url}')
            else:
                if Layer.active_layers.filter(name=self.layer_name).exists():
                    # try getting from DB
                    layer_info, layer_gdf = self.get_from_db()
                    logger.info(f'Getting layer from DB {self.layer_name} from:\n{self.url}')
                else:
                    # Get from Geoserver, store in DB and set in cache
                    layer_info, layer_gdf = self.get_layer_from_geoserver()
                    logger.info(f'Getting layer from GeoServer {self.layer_name} from:\n{self.url}')

        except Exception as e:
            logger.error(f'Error getting layer {self.layer_name} from:\n{self.url}\n{str(e)}')
            raise

        return layer_info, layer_gdf

    def get_layer_from_file(self, filename):
        '''
        Primarily used for Unit Tests

        Returns: layer_info, layer_gdf
        '''
        try:
            # try getting from cache
            layer_info, layer_gdf = self.get_from_cache()
            if layer_gdf is None:
                # Get GeoJSON from file and convert to layer_gdf
                loader = LayerLoader(url=self.url, name=self.layer_name)
                layer = loader.load_layer(filename)
                layer_gdf = layer.to_gdf
                layer_info = self.layer_info(layer)
                self.set_cache(layer_info, layer_gdf)

        except Exception as e:
            logger.error(f'Error getting layer from file {self.layer_name} from:\n{filename}\n{str(e)}')
            raise

        return layer_info, layer_gdf

    def get_layer_from_geoserver(self):
        '''
        Returns: layer_info, layer_gdf
        '''
        try:
            #import ipdb; ipdb.set_trace()
            loader = LayerLoader(url=self.url, name=self.layer_name)
            layer = loader.load_layer()
            layer_gdf = layer.to_gdf
            layer_info = self.layer_info(layer)
            self.set_cache(layer_info, layer_gdf)

        except Exception as e:
            logger.error(f'Error getting layer from GeoServer {self.layer_name} from:\n{self.url}\n{str(e)}')
            raise

        return layer_info, layer_gdf

     
    def get_from_db(self):
        '''
        Get Layer Objects from cache if exists, otherwise get from DB and set the cache
        '''
          
        try:
            layer = Layer.objects.get(name=self.layer_name)
            layer_gdf = layer.to_gdf
            if layer_gdf.crs.srs != settings.CRS:
                layer_gdf.to_crs(settings.CRS, inplace=True)

            layer_info = self.layer_info(layer)
            self.set_cache(layer_info, layer_gdf)
        except Exception as e:
            logger.error(f'Error getting layer {self.layer_name} from DB\n{str(e)}')
            raise

        return layer_info, layer_gdf

    def get_from_cache(self):
        '''
        Get Layer Objects from cache if exists, otherwise get from DB and set the cache
        '''
        # try to get from cached 
        layer_gdf = cache.get(self.LAYER_CACHE.format(self.layer_name))
        layer_info = cache.get(self.LAYER_DETAILS_CACHE.format(self.layer_name))
        return layer_info, layer_gdf

    def clear_cache(self):
        # Clear the cache 
        cache.delete(self.LAYER_CACHE.format(self.layer_name))
        cache.delete(self.LAYER_DETAILS_CACHE.format(self.layer_name))

    def set_cache(self, layer_info, layer_gdf):
        # set the cache 
        cache.set(self.LAYER_CACHE.format(self.layer_name), layer_gdf, settings.CACHE_TIMEOUT)
        cache.set(self.LAYER_DETAILS_CACHE.format(self.layer_name), layer_info, settings.CACHE_TIMEOUT)

    def layer_info(self, layer):
        return dict(
            layer_name=self.layer_name,
            layer_version=layer.version,
            layer_created_date=layer.created_date.strftime(DATETIME_FMT),
            layer_modified_date=layer.modified_date.strftime(DATETIME_FMT),
        )


