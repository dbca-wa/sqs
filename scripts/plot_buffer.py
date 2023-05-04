import os
import sys
import django

#from sqs.utils.das_tests.equals import checkbox_equals
import geopandas as gpd
import json
import json
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('TkAgg')
#matplotlib.use('GTKAgg')
#matplotlib.use('Agg')

#proj_path='/var/www/sqs'
#sys.path.append(proj_path)
#os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sqs.settings")
#django.setup()

#from sqs.components.proposals.models import Proposal
#p=Proposal.objects.last()
#print(p.__dict__)

GEOJSON = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [[[124.12353515624999, -30.391830328088137], [124.03564453125, -31.672083485607377], [126.69433593749999, -31.615965936476076], [127.17773437499999, -29.688052749856787], [124.12353515624999, -30.391830328088137]]]}}]}

# uses a coordinate system on the surface of a sphere
CRS_POLAR = 'EPSG:4326'

# uses a cartesian coordinate system in meters - Used in Australia
CRS_CART = 'EPSG:4462'

def plot(geojson=None, buffer=20000):
    '''
    from scripts.plot_buffer import plot
    plot()
    ------------------------------------------------------
    from sqs.utils.das_tests.equals import checkbox_equals
    plot(geojson=checkbox_equals.GEOJSON, buffer=20000)
    '''
    if geojson is None:
        geojson = GEOJSON
    mpoly = gpd.read_file(json.dumps(geojson))
    mpoly_cart = mpoly.to_crs(CRS_CART)

#    ax = mpoly_m.plot()
#    mpoly_m.buffer(buffer).plot(ax=ax, color='green', alpha=0.6)

    fig, ax = plt.subplots()

    mpoly_m.plot(ax=ax)
    mpoly_m.buffer(buffer).plot(ax=ax, color='yellow', alpha=.5)

    plt.show()

def plot2(geojson=None, buffer=20000):
    '''
    Converts Polar Projection from EPSG:4326 (in deg) to Cartesian Projection (in meters),
    add buffer (in meters) to the new projection, then reverts the buffered polygon to 
    the original projection

    from scripts.plot_buffer import plot2
    plot2()
    ------------------------------------------------------
    from sqs.utils.das_tests.equals import checkbox_equals
    plot(geojson=checkbox_equals.GEOJSON, buffer=20000)
    '''
    if geojson is None:
        geojson = GEOJSON
    mpoly = gpd.read_file(json.dumps(geojson))
    mpoly_polar = mpoly.to_crs(CRS_POLAR)

    mpoly_cart = mpoly.to_crs(CRS_CART)
    mpoly_cart_buffer = mpoly_cart.buffer(buffer)

    mpoly_polar_buffer = mpoly_cart_buffer.to_crs(CRS_POLAR)

    fig, ax = plt.subplots()

    mpoly_polar.plot(ax=ax)
    mpoly_polar_buffer.plot(ax=ax, color='yellow', alpha=.5)

    plt.show()

