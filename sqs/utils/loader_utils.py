from django.contrib.gis.geos import GEOSGeometry, Polygon, MultiPolygon
from django.conf import settings
from django.db import transaction
from django.core.cache import cache

from rest_framework.status import HTTP_200_OK, HTTP_201_CREATED, HTTP_202_ACCEPTED, HTTP_304_NOT_MODIFIED, HTTP_404_NOT_FOUND

import geopandas as gpd
import requests
import json
import os
import sys
from datetime import datetime
import psutil

from sqs.components.gisquery.models import Layer
from sqs.exceptions import LayerProviderException
from sqs.utils import DATE_FMT, DATETIME_FMT, DATETIME_T_FMT

import logging
logger = logging.getLogger(__name__)
logger_stats = logging.getLogger('sys_stats')


class LayerLoader():
    """
    Loads layer into SQS from
    1. API call to Geoserver or 
    2. raw GeoJSON file

    Usage:
        from sqs.utils.loader_utils import LayerLoader
        l=LayerLoader(url,name)
        l.load_layer()
    """

    def __init__(self, url='https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:dpaw_regions&maxFeatures=50&outputFormat=application%2Fjson', name='cddp:dpaw_regions'):
        self.url = url
        self.name = name
        self.data = ''
        
    def retrieve_layer(self):
        """ get GeoJSON from GeoServer
        """
        try:
            res = requests.get('{}'.format(self.url), auth=(settings.LEDGER_USER,settings.LEDGER_PASS), verify=None, timeout=settings.REQUEST_TIMEOUT)
            if res.status_code != HTTP_200_OK:
                res = requests.get('{}'.format(self.url), verify=None, timeout=settings.REQUEST_TIMEOUT)

#            layer_size = round(sys.getsizeof(json.dumps(res.json()))/1024**2, 2)
#            if layer_size > settings.MAX_GEOJSON_SIZE:
#                raise LayerProviderException(f'Layer exceeds max size ({settings.MAX_GEOJSON_SIZE}MB). Layer Size: {layer_size}MB', code='api_layer_retrieve_error' )

            res.raise_for_status()
            return res.json()
        except Exception as e:
            err_msg = f'Error getting layer from API Request {self.name} from:\n{self.url}\n{str(e)}'
            logger.error(err_msg)
            raise LayerProviderException(err_msg, code='api_layer_retrieve_error' )

    @classmethod
    def retrieve_layer_from_file(self, filename):
        try:
            with open(filename) as json_file:
                data = json.load(json_file)
            return data
        except Exception as e:
            err_msg = f'Error getting layer from file {self.name} from:\n{self.url}\n{str(e)}'
            logger.error(err_msg)
            raise LayerProviderException(err_msg, code='file_layer_retrieve_error' )


    def load_layer(self, filename=None, geojson=None):

        try:
            #raise Exception('my exception')
            layer = None
            if filename is not None:
                # get GeoJSON from file
                geojson = self.retrieve_layer_from_file(filename)
            elif geojson is None:
                # get GeoJSON from GeoServer
                geojson = self.retrieve_layer()

            #layer_gdf1 = gpd.read_file(json.dumps(geojson))
            # Create gdf from GEOJSON
            layer_gdf1 = gpd.GeoDataFrame.from_features(geojson['features'])
            #layer_gdf1.set_crs('EPSG:4283', inplace=True)

            qs_layer = Layer.objects.filter(name=self.name)
            with transaction.atomic():
                if len(qs_layer)==1:
                    # check if this layer already exists in DB. If it does exist, 
                    # check if there is a difference between existing layer and new layer from GeoServer.
                    # Only save new layer if its different.

                    layer = qs_layer[0]
                    layer_updated = False
                    if layer_is_unchanged(layer_gdf1, layer.to_gdf):
                        # no change in geojson
                        if layer.active == False:
                            # if not already active, set active
                            layer.active = True
                            layer_updated = True

                        if layer.url != self.url:
                            # url in masterlist_question may have been updated!
                            layer.url = True
                            layer_updated = True

                        self.data = dict(status=HTTP_304_NOT_MODIFIED, data=f'Layer not updated (no change to existing layer in DB): {self.name}')
                    else:
                        #dt_str = datetime.now().strftime(DATETIME_T_FMT)
                        dt_str = datetime.now().strftime(DATE_FMT)
                        path = f'{settings.DATA_STORE}/{self.name}/{dt_str}'
                        if not os.path.exists(path):
                            os.makedirs(path)

                        #filename = f'{settings.DATA_STORE}/{self.name}_{dt_str}.geojson'
                        filename = f'{path}/{self.name}.geojson'
                        with open(filename, 'w') as f:
                            json.dump(geojson, f)

                        # save attr_values
                        attributes = layer_gdf1.loc[:, layer_gdf1.columns != 'geometry'].columns.to_list()
                        attr_values = []
                        data = layer_gdf1[attributes].to_json()
                        for attr in attributes:
                            values = list(set(json.loads(data)[attr].values()))
                            attr_values.append(dict(attribute=attr, values=values))

                        layer.url = self.url
                        layer.geojson_file = filename
                        layer.attr_values = attr_values
                        layer.active = True

                        self.data = dict(status=HTTP_200_OK, data=f'Layer updated: {self.name}')
                        layer_updated = True

                    if layer_updated:
                        layer.save()
                else:
                    # Layer does not exist in DB, so create
                    #filename=f'{settings.DATA_STORE}/{self.name}_{dt_str}.geojson'
                    #dt_str = datetime.now().strftime(DATETIME_T_FMT)
                    dt_str = datetime.now().strftime(DATE_FMT)
                    path=f'{settings.DATA_STORE}/{self.name}/{dt_str}'
                    if not os.path.exists(path):
                        os.makedirs(path)

                    filename=f'{path}/{self.name}.geojson'
                    with open(filename, 'w') as f:
                        #json.dump(geojson, f, ensure_ascii=False)
                        json.dump(geojson, f)

                    #layer = Layer.objects.create(name=self.name, url=self.url, geojson=geojson)
                    #layer_gdf = gpd.read_file(layer.geojson_file.path)
                    attributes = layer_gdf1.loc[:, layer_gdf1.columns != 'geometry'].columns.to_list()
                    #attr_values = [layer_gdf1[col].dropna().unique().tolist() for col in attributes]

                    attr_values = []
                    data = layer_gdf1[attributes].to_json()
                    for attr in attributes:
                        values = list(set(json.loads(data)[attr].values()))
                        attr_values.append(dict(attribute=attr, values=values))

                    layer = Layer.objects.create(name=self.name, url=self.url, geojson_file=filename, attr_values=attr_values)

                    self.data = dict(status=HTTP_201_CREATED, data=f'Layer created: {self.name}')

                logger.info(self.data)

        except Exception as e: 
            err_msg = f'Error getting layer from GeoServer {self.name} from:\n{self.url}\n{str(e)}'
            logger.error(err_msg)
            raise LayerProviderException(err_msg, code='load_layer_retrieve_error' )
        
        return  layer


def layer_is_unchanged(gdf1, gdf2):
    try:
        gdf1 = gdf1.reindex(sorted(gdf1.columns), axis=1)
        gdf2 = gdf2.reindex(sorted(gdf2.columns), axis=1)
        return gdf1.loc[:, ~gdf1.columns.isin(['id', 'md5_rowhash'])].equals(gdf2.loc[:, ~gdf2.columns.isin(['id', 'md5_rowhash'])])
    except Exception as e:
        logger.error(e)

    return False



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
        #self.layer_cached = False
        self.layer_geojson = None

    def get_layer(self, from_geoserver=True):
        '''
        Returns: layer_info, layer_gdf
        '''
        try:
            # try getting from cache
            logger.info(f'Retrieving Layer {self.layer_name} ...')
            print_system_memory_stats()
#            layer_info, layer_gdf = self.get_from_cache()
#            if layer_gdf is not None:
#                logger.info(f'Layer retrieved from cache {self.layer_name}')
#            else:
#                if Layer.active_layers.filter(name=self.layer_name).exists():
#                    # try getting from DB
#                    layer_info, layer_gdf = self.get_from_db()
#                    logger.info(f'Layer retrieved from DB {self.layer_name}')
#                elif from_geoserver:
#                        # Get from Geoserver, store in DB and set in cache
#                        layer_info, layer_gdf = self.get_layer_from_geoserver()
#                        logger.info(f'Layer retrieved from GeoServer {self.layer_name} - from:\n{self.url}')

            if Layer.active_layers.filter(name=self.layer_name).exists():
                # try getting from DB
                layer_info, layer_gdf = self.get_from_db()
                if layer_gdf is not None:
                    logger.info(f'Layer retrieved from DB {self.layer_name}')
            elif from_geoserver:
                # Get from Geoserver, store in DB and set in cache
                layer_info, layer_gdf = self.get_layer_from_geoserver()
                if layer_gdf is not None:
                    logger.info(f'Layer retrieved from GeoServer {self.layer_name} - from:\n{self.url}')


        except Exception as e:
            err_msg = f'Error getting layer {self.layer_name} from:\n{self.url}\n{str(e)}'
            logger.error(err_msg)
            raise LayerProviderException(err_msg, code='get_layer_retrieve_error' )

        return layer_info, layer_gdf

    def get_layer_from_file(self, filename):
        '''
        Primarily used for Unit Tests

        Returns: layer_info, layer_gdf
        '''
        try:
            # try getting from cache
#            layer_info, layer_gdf = self.get_from_cache()
#            if layer_gdf is None:
#                # Get GeoJSON from file and convert to layer_gdf
#                loader = LayerLoader(url=self.url, name=self.layer_name)
#                layer = loader.load_layer(filename)
#                layer_gdf = layer.to_gdf
#                layer_info = self.layer_info(layer)
#                #self.set_cache(layer_info, layer_gdf)
#                self.set_cache(layer_info, layer.geojson)

            loader = LayerLoader(url=self.url, name=self.layer_name)
            layer = loader.load_layer(filename)
            if self.exclude_layer(layer):
                return None, None 

            layer_gdf = layer.to_gdf
            layer_info = self.layer_info(layer)

        except Exception as e:
            err_msg = f'Error getting layer from file {self.layer_name} from:\n{filename}\n{str(e)}'
            logger.error(err_msg)
            raise LayerProviderException(err_msg, code='file_layer_retrieve_error' )

        return layer_info, layer_gdf

    def get_layer_from_geoserver(self):
        '''
        Returns: layer_info, layer_gdf
        '''
        try:
            loader = LayerLoader(url=self.url, name=self.layer_name)
            layer = loader.load_layer()
            if self.exclude_layer(layer):
                return None, None 

            layer_gdf = layer.to_gdf
            layer_info = self.layer_info(layer)
            #self.set_cache(layer_info, layer_gdf)
#            self.set_cache(layer_info, layer.geojson)

        except Exception as e:
            err_msg = f'Error getting layer from GeoServer {self.layer_name} from:\n{self.url}\n{str(e)}'
            logger.error(err_msg)
            raise LayerProviderException(err_msg, code='geoserver_layer_retrieve_error' )

        return layer_info, layer_gdf

     
    def get_from_db(self):
        '''
        Get Layer Objects from cache if exists, otherwise get from DB and set the cache
        '''
          
        try:
            layer = Layer.objects.get(name=self.layer_name)
            if self.exclude_layer(layer):
                return None, None 

            layer_gdf = layer.to_gdf

            layer_info = self.layer_info(layer)
            #self.set_cache(layer_info, layer_gdf)
#            self.set_cache(layer_info, layer.geojson)

        except Exception as e:
            err_msg = f'Error getting layer {self.layer_name} from DB\n{str(e)}'
            logger.error(err_msg)
            raise LayerProviderException(err_msg, code='db_layer_retrieve_error' )

        return layer_info, layer_gdf

#    def get_from_cache(self):
#        '''
#        Get GeoJSON from cache if exists then creates a gdf form the GeoJSON
#        '''
#        # try to get from cached 
#        #layer_gdf = cache.get(self.LAYER_CACHE.format(self.layer_name))
#        self.layer_geojson = cache.get(self.LAYER_CACHE.format(self.layer_name))
#        layer_info = cache.get(self.LAYER_DETAILS_CACHE.format(self.layer_name))
#
#        layer_gdf = gpd.read_file(json.dumps(self.layer_geojson)) if self.layer_geojson else None 
#        self.layer_cached = True if layer_gdf is not None else False
#        return layer_info, layer_gdf
#
#    def clear_cache(self):
#        # Clear the cache 
#        cache.delete(self.LAYER_CACHE.format(self.layer_name))
#        cache.delete(self.LAYER_DETAILS_CACHE.format(self.layer_name))
#
#    def set_cache(self, layer_info, layer_geojson):
#        # set the cache 
#        cache.set(self.LAYER_CACHE.format(self.layer_name), layer_geojson, settings.CACHE_TIMEOUT)
#        cache.set(self.LAYER_DETAILS_CACHE.format(self.layer_name), layer_info, settings.CACHE_TIMEOUT)

    def layer_info(self, layer):
        return dict(
            layer_name=self.layer_name,
            layer_version=layer.version,
            layer_created_date=layer.created_date.strftime(DATETIME_FMT),
            layer_modified_date=layer.modified_date.strftime(DATETIME_FMT),
        )

    def layer_size(self, layer_obj):
        ''' Returns the GeoJSON size in MB '''
        return round(layer_obj.geojson_file.size/1024**2, 2)


    def exclude_layer(self, layer_obj):
        '''  Exclude layer if layer size (in MB) exceeds settings.MAX_GEOJSON_SIZE '''
        if settings.MAX_GEOJSON_SIZE is not None and self.layer_size(layer_obj) > settings.MAX_GEOJSON_SIZE:
            logger.warn(f'Excluding layer {layer_obj.name} because it exceeds max. size {settings.MAX_GEOJSON_SIZE}MB')
            return True
        return False

def get_layer_size(layers=None):
    ''' Prints Cached Layer Sizes in MB 

        from sqs.utils.loader_utils import get_layer_size
        get_layer_size()
        get_layer_size(['CPT_DBCA_LEGISLATED_TENURE'])

        List files by size, in KB/MB
            ls -lrshS ../data_store
    '''
    l= []
    if layers is None:
        layers = list(Layer.objects.all().values_list('name', flat=True))

    for layer in layers:
        provider = DbLayerProvider(layer, '')
        layer_info, layer_gdf = provider.get_layer()
        if layer_info:
            # layer is from cache
            size = provider.layer_size()
            l.append(dict(layer_name=layer, size=size))
    
    for item in l:
        print(f'{item["size"]}\t{item["layer_name"]}')


def print_system_memory_stats():
    info = psutil.virtual_memory()
    mem_avail_perc = round(psutil.virtual_memory().available * 100 / psutil.virtual_memory().total, 2)
    mem_used_perc = round(psutil.virtual_memory().percent, 2)
    cpu_used_perc = round(psutil.cpu_percent(), 2)

    #logger.info(f'{info}\nMem Avail %: {mem_avail_perc}, Mem Used %: {mem_used_perc}, CPU Used %: {cpu_used_perc}')
    #logger.info(f'Mem Avail %: {mem_avail_perc}, Mem Used %: {mem_used_perc}, CPU Used %: {cpu_used_perc}')
    logger_stats.info(f'Mem Avail %: {mem_avail_perc}, Mem Used %: {mem_used_perc}, CPU Used %: {cpu_used_perc}')

