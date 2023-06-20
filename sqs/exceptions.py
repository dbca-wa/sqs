#from django.core.exceptions import Exception
from rest_framework.exceptions import APIException,PermissionDenied

class LayerSaveException(Exception):
    message = 'Error loading new layer from GeoServer' 

#class LayerProviderException(Exception):
#    pass

class LayerProviderException(APIException):
    status_code = 400
    default_detail = 'Error getting layer from GeoServer'
    default_code = 'layer_retrieve_error'

    def __init__(self, detail=None, code=None):
        if detail is not None:
            self.detail = detail
        else:
            self.detail = self.default_detail

        if code is not None:
            self.code = code
        else:
            self.code = self.default_code

