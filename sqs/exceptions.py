#from django.core.exceptions import Exception

class LayerSaveException(Exception):
    message = 'Error loading new layer from GeoServer' 

class LayerProviderException(Exception):
    pass

