from django.contrib.gis.geos import GEOSGeometry, Polygon, MultiPolygon
from django.conf import settings
from django.db import transaction
from django.core.cache import cache
from django.db.models import Max

from rest_framework.status import HTTP_200_OK, HTTP_201_CREATED, HTTP_202_ACCEPTED, HTTP_304_NOT_MODIFIED, HTTP_404_NOT_FOUND

import geopandas as gpd
import requests
import json
import os
import sys
import math
from datetime import datetime
from dateutil import parser
import psutil
import gc
from pathlib import Path
from argparse import Namespace
from geojsplit import cli as geojsplit_cli
import ijson

from sqs.components.gisquery.models import Layer, GeoJsonFile
from sqs.exceptions import LayerProviderException
from sqs.utils import HelperUtils, DATE_FMT, DATETIME_FMT, DATETIME_T_FMT

import logging
logger = logging.getLogger(__name__)
logger_stats = logging.getLogger('sys_stats')


#def layer_latest(layer_name):
#    qs = Layer.objects.filter(name=layer_name)
#    if qs.exists():
#        return qs.order_by('-version')[0] if qs.exists() else Layer.objects.none()

class RecentLayerProvider():
    """
    Get a list of recently updated layers from KB - within last <days_ago>. Cross check if they also exist in SQS, return those that do exist.

    Usage:
        from sqs.utils.loader_utils import RecentLayerProvider
        layers_to_update = RecentLayerProvider(days_ago=7).get_layers_to_update()
    """

    def __init__(self, days_ago=7):
        self.days_ago = days_ago
        
    def _recently_updated_layers(self):
        ''' get a list of recently updated layers from KB - within last <days_ago>
        '''
        try:
            #url = f'{settings.KB_RECENT_LAYERS_URL}{self.days_ago}'
            url = settings.KB_RECENT_LAYERS_URL.format(self.days_ago)
            res = requests.get('{}'.format(url), auth=(settings.LEDGER_USER,settings.LEDGER_PASS), verify=None, timeout=settings.REQUEST_TIMEOUT)
            res.raise_for_status()
            return res.json()
        except Exception as e:
            err_msg = f'Error getting recent layers from API Request {url}\n{str(e)}'
            logger.error(err_msg)
            raise LayerProviderException(err_msg, code='api_recent_layer_retrieve_error' )

    def get_layers_to_update(self):
        ''' check which layers have been updated in KB (in last <n-days>), then cross-check with layers in SQS
        '''
        layers = []
        for layer in self._recently_updated_layers():
            layer_name = layer['name']
            updated_at = parser.parse(layer['updated_at'])
            #print(layer_name, updated_at)
            qs = Layer.objects.filter(name=layer_name, modified_at__lt=updated_at)
            if qs:
                layers.append(layer_name)
                #print(qs[0].name, qs[0].modified_at)
        return layers

#    def layer_is_unchanged(layer_name, days_ago):
#       # TODO
#       layers_to_update = get_layers_to_update(days_ago)
#       return True if layer_name in layers_to_update else False


class LayerLoader():
    """
    Loads layer into SQS from
    1. API call to Geoserver or 
    2. raw GeoJSON file

    Usage:
        from sqs.utils.loader_utils import LayerLoader, RecentLayerProvider
        layers_to_update = RecentLayerProvider(days_ago=7).get_layers_to_update()
        l=LayerLoader(name, layers_to_update)
        l.load_layer()
    """

    #def __init__(self, name='CPT_DBCA_REGIONS', layers_to_update=[]):
    def __init__(self, name='CPT_DBCA_REGIONS'):
        self.name = name
        #self.layers_to_update = layers_to_update
        #self.url = f'https://kaartdijin-boodja.dbca.wa.gov.au/api/catalogue/entries/{name}/layer/'
        self.geojson_split_geometry_count = settings.GEOJSON_SPLIT_GEOMETRY_COUNT
        self.url = settings.KB_LAYER_URL.format(name)
        
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

    def split_geojson_file_recursive(
        self, filename, file_prefix=None, round=0, geometry_count=2000, preserve_file=None
    ):
        """
        Splits a GeoJSON file into smaller files with a specified number of geometries.
        If the resulting file is larger than the max size, it will recursively split it further.
        """
        if round > 30 or geometry_count <= 1:
            logger.info(
                f"Max split rounds reached for file {filename}. Skipping further splits."
            )
            return
        try:
            file_path = Path(filename)
            file_size = Path(filename).stat().st_size/1024**2 # MB
            if settings.MAX_GEOJSPLIT_SIZE!=0 and file_size > settings.MAX_GEOJSPLIT_SIZE:
                args = Namespace(
                    geojson=file_path,
                    geometry_count=geometry_count,
                    suffix_length=None,
                    output=None,
                    limit=None,
                    verbose=False,
                    dry_run=False,
                )
                logger.info(
                    f"Splitting GeoJSON file {file_path} into smaller files with {geometry_count} geometries each."
                )
                geojsplit_cli.input_geojson(args=args)
                if preserve_file is None:
                    preserve_file = filename  # Default to the input filename
                logger.info(f"GeoJSON file {file_path} split successfully.")
                try:
                    # file_path.unlink()
                    # logger.info(f"Removed original file {file_path}")
                    if file_path.resolve() != Path(preserve_file).resolve():
                        file_path.unlink()
                        logger.info(f"Removed original file {file_path}")
                    else:
                        logger.info(f"Preserved original file {file_path}")
                except Exception as e:
                    logger.error(f"Error removing original file {file_path}: {str(e)}")
                    raise Exception(f"Error removing original file {file_path}: {str(e)}")

                # directory
                output_dir = file_path.parent
                for file in output_dir.glob(
                    "*.geojson" if file_prefix is None else f"{file_prefix}*.geojson"
                ):
                    if file.resolve() == Path(preserve_file).resolve():
                        logger.info(f"Skipping preserved file in loop: {file.name}")
                        continue
                    file_size = file.stat().st_size
                    file_size_str = (
                        f"{file_size / (1024 * 1024):.2f} MB"
                        if file_size > 1024 * 1024
                        else f"{file_size / 1024:.2f} KB"
                    )
                    logger.info(f"Created split file: {file.name} ({file_size_str})")
                    # rename
                    if (
                        settings.MAX_GEOJSPLIT_SIZE!=0
                        and file_size/1024**2  > settings.MAX_GEOJSPLIT_SIZE
                    ):
                        suffix = file.suffix
                        new_name = f"split_round_{round}_size_{geometry_count}_{file.stem.split('_')[-1]}"
                        new_full_name = f"{new_name}{suffix}"
                        logger.info(f"Renaming file {file.name} to {new_full_name}")
                        os.rename(file, output_dir / f"{new_full_name}")

                        geometry_count = math.ceil(geometry_count / 4)
                        self.split_geojson_file_recursive(
                            output_dir / new_full_name,
                            new_name,
                            round=round + 1,
                            geometry_count=geometry_count,
                            preserve_file=preserve_file,
                        )
                    else:
                        file_rename = file.with_name(
                            f"{file.stem}_round_{round}_size_{file_size_str}.geojson"
                        )
                        file.rename(file_rename)
        except Exception as e:
            err_msg = f"Error splitting geojson to smaller files\n{str(e)}"
            logger.error(err_msg)


    def split_geojson_files_from_file(self, filename):
        ''' Util function to split large geojson file to smaller chunks with given no. of features
            Egi. client usage:
                geojsplit --geometry-count 10000 /path-to-geojson/CPT_DBCA_REGIONS.geojson
        '''
        try:
            file_size = Path(filename).stat().st_size/1024**2 # MB
            print('file size', file_size)
            if settings.MAX_GEOJSPLIT_SIZE!=0 and file_size > settings.MAX_GEOJSPLIT_SIZE:
                num_files = math.ceil(file_size/settings.MAX_GEOJSPLIT_SIZE)
                geometry_count = 2000 # default value
                logger.info('Geometry count %s', geometry_count)
                args = Namespace(geojson=filename, geometry_count=geometry_count, suffix_length=None, output=None, limit=None, verbose=False, dry_run=False)
                logger.info('args defined')
                geojsplit_cli.input_geojson(args=args)
        except Exception as e:
            err_msg = f'Error splitting geojson to smaller files\n{str(e)}'
            logger.error(err_msg)
            raise LayerProviderException(err_msg, code='file_layer_geojsplit_error' )
        
    def split_geojson_files(self, filename, geojson):
        ''' Util function to split large geojson file to smaller chunks with given no. of features
            Egi. client usage:
                geojsplit --geometry-count 10000 /path-to-geojson/CPT_DBCA_REGIONS.geojson
        '''
        try:
            file_size = Path(filename).stat().st_size/1024**2 # MB
            if settings.MAX_GEOJSPLIT_SIZE!=0 and file_size > settings.MAX_GEOJSPLIT_SIZE:
                num_files = math.ceil(file_size/settings.MAX_GEOJSPLIT_SIZE)
                geometry_count = math.ceil(len(geojson['features'])/num_files)
                args = Namespace(geojson=filename, geometry_count=geometry_count, suffix_length=None, output=None, limit=None, verbose=True, dry_run=False)
                geojsplit_cli.input_geojson(args=args)
        except Exception as e:
            err_msg = f'Error splitting geojson to smaller files\n{str(e)}'
            logger.error(err_msg)
            raise LayerProviderException(err_msg, code='file_layer_geojsplit_error' )

    def retrieve_layer_to_file(self):
        """ get GeoJSON from GeoServer
        """
        try:
            #res = requests.get('{}'.format(self.url), auth=(settings.LEDGER_USER,settings.LEDGER_PASS), verify=None, timeout=settings.REQUEST_TIMEOUT)
            # Stream large GeoJSON from GeoServer to file
            with requests.get(
                '{}'.format(self.url),
                auth=(settings.LEDGER_USER, settings.LEDGER_PASS),
                verify=False,
                timeout=settings.REQUEST_TIMEOUT,
                stream=True  # Critical: enables streaming mode
            ) as res:
                if res.status_code == 200:
                    print(f"Downloading GeoJSON for layer {self.name} from {self.url} ...")
                    dt_str = datetime.now().strftime(DATETIME_T_FMT)
                    path=f'{settings.DATA_STORE}/{self.name}/{dt_str}'
                    if not os.path.exists(path):
                        os.makedirs(path)

                    filename=f'{path}/{self.name}.geojson'
                    with open(filename, 'wb') as f:
                        for chunk in res.iter_content(chunk_size=8192):
                            if chunk:  # Filter out keep-alive chunks
                                f.write(chunk)
                    print("GeoJSON successfully written to layer.geojson")
                    return filename
                else:
                    res.raise_for_status()
        except Exception as e:
            err_msg = f'Error getting layer from API Request {self.name} from:\n{self.url}\n{str(e)}'
            logger.error(err_msg)
            raise LayerProviderException(err_msg, code='api_layer_retrieve_error' )

       
    def get_crs_from_file(self, filename):
        import ijson 
        try:
            with open(filename, 'rb') as f:
                parser = ijson.parse(f)
                for prefix, event, value in parser:
                    if prefix == 'crs.properties.name':
                        code = value.split(':')[-1]  # Get '4283' from 'urn:ogc:def:crs::4283'
                        return f"epsg:{code.lower()}"
                print("No CRS field found. Assuming EPSG:4326 (WGS 84)")
                return 'epsg:4326'
        except Exception as e:
            raise Exception(f"Cannot determine CRS from layer {self.name}: {e}")

    
    def convert_value(self,val):
        from decimal import Decimal
        from collections.abc import Iterable
        if isinstance(val, Decimal):
            return float(val)
        elif isinstance(val, Iterable) and not isinstance(val, str):
            return [self.convert_value(v) for v in val]
        else:
            return val
    
    def get_attr_values_stream(self, file_path):
        # import ijson
        from collections import defaultdict
        attr_values = defaultdict(set)

        with open(file_path, 'rb') as f:
            objects = ijson.items(f, 'features.item.properties')
            for props in objects:
                for attr, val in props.items():
                    if not isinstance(attr, str):
                        logger.warning(f'Non-string attribute key encountered: {attr} (type: {type(attr)}). Converting to string.')
                        key = str(attr)
                    else:
                        key = attr
                    if isinstance(val, list):
                        logger.warning(f'List value encountered for attribute "{key}". Adding each element separately.')
                        for v in val:
                            attr_values[key].add(self.convert_value(v))
                    else:
                        attr_values[key].add(self.convert_value(val))
                    # try:
                    #     # attr_values[attr].add(val)
                    #     attr_values[attr].add(self.convert_value(val))
                    # except Exception as e:
                    #     logger.error(f'Error processing attribute {attr} with value {val}: {e}')
                    #     continue
        output = []
        for attr, vals in attr_values.items():
            cleaned = [self.convert_value(v) for v in vals]
            cleaned_sorted = sorted(cleaned, key=lambda x: (str(type(x)), str(x)))
            output.append({
                "attribute": attr,
                "values": cleaned_sorted
            })

        return output
    
    def load_layer(self, filename=None, geojson=None):

        HelperUtils.force_gc()
        layer = None
        if self.name in settings.KB_EXCLUDE_LAYERS:
            logger.info('Layer {self.name} is in EXCLUSION list. Layer not created/updated')
            return layer

        try:
            if filename is None:
                # get GeoJSON from file
                logger.info('Retrieving layer from geoserver %s', filename)
                filename = self.retrieve_layer_to_file()
            logger.info('Getting crs from file: %s', filename)
            crs = self.get_crs_from_file(filename)

            qs_layer = Layer.objects.filter(name=self.name)
            with transaction.atomic():
                # Layer already saved to Geojson file so we need to split it to smaller files
                logger.info('splitting layer to smaller files %s', filename)
                # self.split_geojson_files_from_file(filename)
                self.split_geojson_file_recursive(filename, geometry_count=self.geojson_split_geometry_count)

                logger.info('Getting attribute values from files %s', filename)
                attr_values =self.get_attr_values_stream(filename)

                qs = Layer.objects.filter(name=self.name)
                version = qs.aggregate(version_max=Max('version'))['version_max'] + 1 if qs else 0

                logger.info('creating a layer object %s', filename)
                layer, created = Layer.objects.update_or_create(
                    name=self.name,
                    defaults={'crs': crs, 'version': version, 'url': self.url, 'attr_values': attr_values},
                )

                geojson_file = GeoJsonFile.objects.create(layer=layer, geojson_file=filename)

                msg = dict(status=HTTP_201_CREATED, data=f'Layer created/updated: {self.name}')
                logger.info(msg)

        except Exception as e: 
            err_msg = f'Error getting layer from GeoServer {self.name} from:\n{self.url}\n{str(e)}'
            logger.error(err_msg)
            raise LayerProviderException(err_msg, code='load_layer_retrieve_error' )
        
        return  layer

    #def load_layer(self, filename=None, geojson=None, force_load=False):
    def load_layer_orig(self, filename=None, geojson=None):

        def get_crs_from_geojson(geojson):
            try:
                crs = geojson['crs']['properties']['name'].split('EPSG::')[-1]
                return f'epsg:{crs}'
            except KeyError as ke:
                raise Exception(f'Cannot determine CRS from layer {self.name}: {ke}')

        def get_attr_values(geojson):
            #with open('data_store/CPT_DBCA_REGIONS/20240823T141341/CPT_DBCA_REGIONS.geojson') as fid:
            #    geojson = json.load(fid)
            attr_values = [] 
            attributes=list(geojson['features'][0]['properties'].keys())
            for attr in attributes:
                try:
                    values = list(set([f['properties'][attr] for f in geojson['features']])) # list(set([...])) --> return unique values
                    attr_values.append(dict(attribute=attr, values=values))
                except Exception as ex:
                    logger.error(f'Error setting attributes for layer, omitting attr {attr}: {ex}')

            return attr_values

        HelperUtils.force_gc()
        layer = None
        if self.name in settings.KB_EXCLUDE_LAYERS:
            logger.info('Layer {self.name} is in EXCLUSION list. Layer not created/updated')
            return layer

        try:
            if filename is not None:
                # get GeoJSON from file
                geojson = self.retrieve_layer_from_file(filename)
            elif geojson is None:
                # get GeoJSON from GeoServer
                geojson = self.retrieve_layer()

            crs = get_crs_from_geojson(geojson)

            #layer_gdf1 = gpd.read_file(json.dumps(geojson))
            # Create gdf from GEOJSON
            #layer_gdf1 = gpd.GeoDataFrame.from_features(geojson['features'])
            #layer_gdf1.set_crs(crs, inplace=True)

            qs_layer = Layer.objects.filter(name=self.name)
            with transaction.atomic():
                # create/update layer
                dt_str = datetime.now().strftime(DATETIME_T_FMT)
                path=f'{settings.DATA_STORE}/{self.name}/{dt_str}'
                if not os.path.exists(path):
                    os.makedirs(path)

                filename=f'{path}/{self.name}.geojson'
                with open(filename, 'w') as f:
                    json.dump(geojson, f)
                #filename='data_store/CPT_DBCA_LEGISLATED_TENURE/20240829T142718/CPT_DBCA_LEGISLATED_TENURE.geojson'
                self.split_geojson_files(filename, geojson)

#                file_size = Path(filename).stat().st_size/1024**2 # MB
#                if settings.MAX_GEOJSPLIT_SIZE!=0 and file_size > settings.MAX_GEOJSPLIT_SIZE:
#                    file_size_split = math.ceil(file_size/settings.MAX_GEOJSPLIT_SIZE)
#                    geometry_count = math.ceil(len(geojson['features'])/file_size_split)
#                    args = Namespace(geojson=filename, geometry_count=geometry_count, suffix_length=None, output=None, limit=None, verbose=False, dry_run=False)
#                    geojsplit_cli.input_geojson(args=args)

#                attributes = layer_gdf1.loc[:, layer_gdf1.columns != 'geometry'].columns.to_list()
#
#                attr_values = []
#                data = layer_gdf1[attributes].to_json()
#                for attr in attributes:
#                    values = list(set(json.loads(data)[attr].values()))
#                    attr_values.append(dict(attribute=attr, values=values))
                attr_values = get_attr_values(geojson)

                qs = Layer.objects.filter(name=self.name)
                version = qs.aggregate(version_max=Max('version'))['version_max'] + 1 if qs else 0

                #layer = Layer.objects.create(name=self.name, version=version, url=self.url, attr_values=attr_values)
                layer, created = Layer.objects.update_or_create(
                    name=self.name,
                    defaults={'crs': crs, 'version': version, 'url': self.url, 'attr_values': attr_values},
                )

                geojson_file = GeoJsonFile.objects.create(layer=layer, geojson_file=filename)

                msg = dict(status=HTTP_201_CREATED, data=f'Layer created/updated: {self.name}')
                logger.info(msg)

        except Exception as e: 
            err_msg = f'Error getting layer from GeoServer {self.name} from:\n{self.url}\n{str(e)}'
            logger.error(err_msg)
            raise LayerProviderException(err_msg, code='load_layer_retrieve_error' )
        
        return  layer


#def layer_is_unchanged(gdf1, gdf2):
#    try:
#        gdf1 = gdf1.reindex(sorted(gdf1.columns), axis=1)
#        gdf2 = gdf2.reindex(sorted(gdf2.columns), axis=1)
#        return gdf1.loc[:, ~gdf1.columns.isin(['id', 'md5_rowhash'])].equals(gdf2.loc[:, ~gdf2.columns.isin(['id', 'md5_rowhash'])])
#    except Exception as e:
#        logger.error(e)
#
#    return False


#def layer_exists(layer_name):
#    ''' Check that the layer_obj exists in DB, and also that the file exists on file storage '''
#    qs_layer = Layer.latest.filter(name=layer_name)
#    if qs_layer.exists() and qs_layer[0].geojson_file is not None:
#        return True
#    return False   


#def layer_is_unchanged(layer_name, days_ago):
#    # TODO
#    layers_to_update = get_layers_to_update(days_ago)
#    return True if layer_name in layers_to_update else False



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

            #if Layer.active_layers.filter(name=self.layer_name).exists():
            if Layer.objects.filter(name=self.layer_name).exists():
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

            loader = LayerLoader(name=self.layer_name)
            layer = loader.load_layer(filename=filename, force_load=True)
            if self.exclude_layer(layer):
                return None, None 

            layer_gdf = layer.to_gdf(all_features=True)
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
            loader = LayerLoader(name=self.layer_name)
            layer = loader.load_layer()
            if self.exclude_layer(layer):
                return None, None 

            layer_gdf = layer.to_gdf(all_features=True)
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

            layer_gdf = layer.to_gdf(all_features=True)

            layer_info = self.layer_info(layer)
            #self.set_cache(layer_info, layer_gdf)
#            self.set_cache(layer_info, layer.geojson)

        except Exception as e:
            err_msg = f'Error getting layer {self.layer_name} from DB\n{str(e)}'
            logger.error(err_msg)
            raise LayerProviderException(err_msg, code='db_layer_retrieve_error' )

        return layer_info, layer_gdf

    def get_layer_generator(self):
        '''
        Return generator to load geojson from filesystem in batches/parts
        '''
          
        try:
            layer = Layer.objects.get(name=self.layer_name)
            if self.exclude_layer(layer):
                return None, None 

            #layer_gdf = layer.to_gdf
            layer_gen = layer.geojson_generator()

            layer_info = self.layer_info(layer)
            #self.set_cache(layer_info, layer_gdf)
#            self.set_cache(layer_info, layer.geojson)

        except Exception as e:
            err_msg = f'Error getting layer {self.layer_name} from DB\n{str(e)}'
            logger.error(err_msg)
            raise LayerProviderException(err_msg, code='db_layer_retrieve_error' )

        return layer_info, layer_gen


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
            layer_crs=layer.crs,
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


def print_system_memory_stats(msg=None):
    if settings.SHOW_SYS_MEM_STATS:
        info = psutil.virtual_memory()
        avail_mem = int(psutil.virtual_memory().available / 1024**2)
        total_mem = int(psutil.virtual_memory().total /1024**2)
        mem_avail_perc = round(avail_mem * 100 / total_mem, 2)
        mem_used_perc = round(psutil.virtual_memory().percent, 2)
        cpu_used_perc = round(psutil.cpu_percent(), 2)

        logger_stats.debug(f'{msg} - Mem Avail %: {mem_avail_perc} ({avail_mem:,}/{total_mem:,} MB), CPU Used %: {cpu_used_perc}')

