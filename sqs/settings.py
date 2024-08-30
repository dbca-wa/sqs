from django.core.exceptions import ImproperlyConfigured

import os, hashlib
import sys
import confy
from confy import env, database
import dj_database_url

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
confy.read_environment_file(BASE_DIR+"/.env")
os.environ.setdefault("BASE_DIR", BASE_DIR)

#from ledger_api_client.settings_base import *
SECRET_KEY = env('SECRET_KEY')
DEBUG = env('DEBUG', False)
CSRF_COOKIE_SECURE = env('CSRF_COOKIE_SECURE', False)
SESSION_COOKIE_SECURE = env('SESSION_COOKIE_SECURE', False)
if DEBUG:
    ALLOWED_HOSTS = ['*']
else:
    ALLOWED_HOSTS = env('ALLOWED_HOSTS', [])

ROOT_URLCONF = 'sqs.urls'
SITE_ID = 1
DEPT_DOMAINS = env('DEPT_DOMAINS', ['dpaw.wa.gov.au', 'dbca.wa.gov.au'])
SYSTEM_MAINTENANCE_WARNING = env('SYSTEM_MAINTENANCE_WARNING', 24) # hours
LEDGER_USER = env('LEDGER_USER', 'asi@dbca.wa.gov.au')
LEDGER_PASS = env('LEDGER_PASS')
#SHOW_DEBUG_TOOLBAR = env('SHOW_DEBUG_TOOLBAR', False)
BUILD_TAG = env('BUILD_TAG', hashlib.md5(os.urandom(32)).hexdigest())  # URL of the Dev app.js served by webpack & express

REQUEST_TIMEOUT = env('REQUEST_TIMEOUT', 300) # 20 secs

CACHE_TIMEOUT_1_MINUTE = 60
CACHE_TIMEOUT_5_MINUTES = 60 * 5
CACHE_TIMEOUT_2_HOURS = 60 * 60 * 2
CACHE_TIMEOUT_24_HOURS = 60 * 60 * 24

# This cache is updated first Sunday of each month by cron. Also, updated if an existing layer 
# is updated (on demand, and by nightly cron 'manage.py update_active_layers')
CACHE_TIMEOUT = env('CACHE_TIMEOUT', None) 

# API Request cache
REQUEST_CACHE_TIMEOUT = env('REQUEST_CACHE_TIMEOUT', 60*10) # 10 mins
REQUEST_PARTIAL_CACHE_TIMEOUT = env('REQUEST_PARTIAL_CACHE_TIMEOUT', 60*2) # 2 mins

CHECK_APIURL_TOKEN = env('CHECK_APIURL_TOKEN', True)
CHECK_IP = env('CHECK_IP', True)

#USE_SQS_CACHING = env('USE_SQS_CACHING', True)

# Use 'epsg:4326' as projected coordinate system - 'epcg:4326' coordinate system is in meters (Then the buffer distance will be in meters)
CRS = env('CRS', 'epsg:4326')
CRS_CARTESIAN = env('CRS_CARTESIAN', 'epsg:3043')
#GEOM_AREA_LENGTH_FILTER = env('GEOM_AREA_LENGTH_FILTER', 1)
DEFAULT_BUFFER = env('DEFAULT_BUFFER', -1) # reduce the polygon perimeter - in meters
MAX_GEOJSON_SIZE = env('MAX_GEOJSON_SIZE', None) # MB
GEOJSON_BATCH_SIZE = env('GEOJSON_BATCH_SIZE', 1000)
SHOW_SYS_MEM_STATS = env('SHOW_SYS_MEM_STATS', False)
USE_LAYER_STREAMING = env('USE_LAYER_STREAMING', False)
USE_LAYER_SPLIT_FILES = env('USE_LAYER_SPLIT_FILES', True)
MAX_GEOJSPLIT_SIZE = env('MAX_GEOJSPLIT_SIZE', 75) # MB
GC_ITER_LOOP = env('GC_ITER_LOOP', 5)
MAX_RETRIES = env('MAX_RETRIES', 3)
STALE_TASKS_DAYS = env('STALE_TASKS_DAYS', 7)
LOG_ELAPSED_TIME = env('LOG_ELAPSED_TIME', False)

KB_BASE_URL = env('KB_BASE_URL', 'https://kaartdijin-boodja.dbca.wa.gov.au/api/')
KB_RECENT_LAYERS_URL = KB_BASE_URL + 'catalogue/entries/recent/?days_ago={}'
KB_LAYER_URL = KB_BASE_URL + 'catalogue/entries/{}/layer/'
KB_EXCLUDE_LAYERS = env('KB_EXCLUDE_LAYERS', [])
#KB_RECENT_LAYERS_URL = env('KB_RECENT_LAYERS_URL', 'https://kaartdijin-boodja.dbca.wa.gov.au/api/catalogue/entries/recent/?days_ago=')
DATA_STORE = env('DATA_STORE', 'data_store') # MB
if not os.path.exists(DATA_STORE):
    os.makedirs(DATA_STORE)

LANGUAGE_CODE = 'en-AU'
TIME_ZONE = 'Australia/Perth'
USE_I18N = True
USE_L10N = True
USE_TZ = True

# Custom Email Settings
EMAIL_BACKEND = 'django.core.mail.backends.console.EmailBackend' if env('CONSOLE_EMAIL_BACKEND', False) else 'sqs.backend_email.SqsEmailBackend'
PRODUCTION_EMAIL = env('PRODUCTION_EMAIL', False)
# Intercept and forward email recipient for non-production instances
# Send to list of NON_PROD_EMAIL users instead
EMAIL_INSTANCE = env('EMAIL_INSTANCE','PROD')
NON_PROD_EMAIL = env('NON_PROD_EMAIL')
if not PRODUCTION_EMAIL:
    if not NON_PROD_EMAIL:
        raise ImproperlyConfigured('NON_PROD_EMAIL must not be empty if PRODUCTION_EMAIL is set to False')
    if EMAIL_INSTANCE not in ['PROD','DEV','TEST','UAT']:
        raise ImproperlyConfigured('EMAIL_INSTANCE must be either "PROD","DEV","TEST","UAT"')
    if EMAIL_INSTANCE == 'PROD':
        raise ImproperlyConfigured('EMAIL_INSTANCE cannot be \'PROD\' if PRODUCTION_EMAIL is set to False')

STATIC_URL = '/static/'

#INSTALLED_APPS += [
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.gis',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django_extensions',
    'django_cron',

    'reversion_compare',
    'bootstrap3',
    'sqs',
    'sqs.components.gisquery',
    'sqs.components.api',
    'reversion',
    'rest_framework',
    'rest_framework.authtoken',
    'rest_framework_gis',
    'rest_framework_swagger',
    #'silk',
    'appmonitor_client',
]

ADD_REVERSION_ADMIN=True

# maximum number of days allowed for a booking
WSGI_APPLICATION = 'sqs.wsgi.application'

'''REST_FRAMEWORK = {
    'DEFAULT_PERMISSION_CLASSES': (
        'sqs.perms.OfficerPermission',
    ),
    'DEFAULT_SCHEMA_CLASS': 'rest_framework.schemas.coreapi.AutoSchema'
}'''

REST_FRAMEWORK = {
    'DEFAULT_RENDERER_CLASSES': (
        'rest_framework.renderers.JSONRenderer',
        'rest_framework.renderers.BrowsableAPIRenderer',
        #'rest_framework_datatables.renderers.DatatablesRenderer',
    ),
    'DEFAULT_SCHEMA_CLASS': 'rest_framework.schemas.coreapi.AutoSchema',
}

#MIDDLEWARE_CLASSES = [
MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    #'silk.middleware.SilkyMiddleware',
    'dbca_utils.middleware.SSOLoginMiddleware',
    #'sqs.middleware.CacheControlMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',
]
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            os.path.join(BASE_DIR, 'sqs', 'templates'),
            os.path.join(BASE_DIR, 'sqs', 'templates', 'sqs'),
        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]
#TEMPLATES[0]['DIRS'].append(os.path.join(BASE_DIR, 'sqs', 'templates'))
#TEMPLATES[0]['DIRS'].append(os.path.join(BASE_DIR, 'sqs','components','emails', 'templates'))

BOOTSTRAP3 = {
    'jquery_url': '//static.dpaw.wa.gov.au/static/libs/jquery/2.2.1/jquery.min.js',
    #'base_url': '//static.dpaw.wa.gov.au/static/libs/twitter-bootstrap/3.3.6/',
    'base_url': '/static/ledger/',
    'css_url': None,
    'theme_url': None,
    'javascript_url': None,
    'javascript_in_head': False,
    'include_jquery': False,
    'required_css_class': 'required-form-field',
    'set_placeholder': False,
}

del BOOTSTRAP3['css_url']
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.filebased.FileBasedCache',
        'LOCATION': os.path.join(BASE_DIR, 'sqs', 'cache'),
    }
}

STATIC_ROOT=os.path.join(BASE_DIR, 'staticfiles')

DEV_STATIC = env('DEV_STATIC',False)
DEV_STATIC_URL = env('DEV_STATIC_URL')
if DEV_STATIC and not DEV_STATIC_URL:
    raise ImproperlyConfigured('If running in DEV_STATIC, DEV_STATIC_URL has to be set')
DATA_UPLOAD_MAX_NUMBER_FIELDS = None

# Department details
SYSTEM_NAME = env('SYSTEM_NAME', 'Spatial Query System')
SYSTEM_NAME_SHORT = env('SYSTEM_NAME_SHORT', 'SQS')
SITE_PREFIX = env('SITE_PREFIX')
SITE_DOMAIN = env('SITE_DOMAIN')
SUPPORT_EMAIL = env('SUPPORT_EMAIL', 'sqs@' + SITE_DOMAIN).lower()
DEP_URL = env('DEP_URL','www.' + SITE_DOMAIN)
DEP_PHONE = env('DEP_PHONE','(08) 9219 9978')
DEP_PHONE_SUPPORT = env('DEP_PHONE_SUPPORT','(08) 9219 9000')
DEP_FAX = env('DEP_FAX','(08) 9423 8242')
DEP_POSTAL = env('DEP_POSTAL','Locked Bag 104, Bentley Delivery Centre, Western Australia 6983')
DEP_NAME = env('DEP_NAME','Department of Biodiversity, Conservation and Attractions')
DEP_NAME_SHORT = env('DEP_NAME_SHORT','DBCA')
BRANCH_NAME = env('BRANCH_NAME','Office of Information Management')
DEP_ADDRESS = env('DEP_ADDRESS','17 Dick Perry Avenue, Kensington WA 6151')
SITE_URL = env('SITE_URL', 'https://' + SITE_PREFIX + '.' + SITE_DOMAIN)
PUBLIC_URL=env('PUBLIC_URL', SITE_URL)
DEFAULT_FROM_EMAIL = env('DEFAULT_FROM_EMAIL', 'no-reply@' + SITE_DOMAIN).lower()
MEDIA_APP_DIR = env('MEDIA_APP_DIR', 'sqs')
ADMIN_GROUP = env('ADMIN_GROUP', 'SQS Admin')
CRON_RUN_AT_TIMES = env('CRON_RUN_AT_TIMES', '04:05')
CRON_EMAIL = env('CRON_EMAIL', 'cron@' + SITE_DOMAIN).lower()
# for ORACLE Job Notification - override settings_base.py
#PAYMENT_SYSTEM_ID = env('PAYMENT_SYSTEM_ID', 'S999')
EMAIL_FROM = DEFAULT_FROM_EMAIL
NOTIFICATION_EMAIL=env('NOTIFICATION_EMAIL')
CRON_NOTIFICATION_EMAIL = env('CRON_NOTIFICATION_EMAIL', NOTIFICATION_EMAIL).lower()
EMAIL_HOST = env('EMAIL_HOST', 'smtp.lan.fyi')

#SILKY_PYTHON_PROFILER = True

# Database
DATABASES = {
    # Defined in the DATABASE_URL env variable.
    'default': database.config(),
}

CRON_CLASSES = [
    'appmonitor_client.cron.CronJobAppMonitorClient',
]

BASE_URL=env('BASE_URL')

if not os.path.exists(os.path.join(BASE_DIR, 'logs')):
    os.mkdir(os.path.join(BASE_DIR, 'logs'))
LOGGING = {
    'version': 1,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': env('LOG_CONSOLE_LEVEL', 'INFO'),
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
        'file': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(BASE_DIR, 'logs', 'sqs.log'),
            'formatter': 'verbose',
            'maxBytes': 5242880
        },
        'debug': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(BASE_DIR, 'logs', 'sys_stats.log'),
            'formatter': 'verbose',
            'maxBytes': 5242880
        },

    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': env('LOG_CONSOLE_LEVEL', 'WARNING'),
            'propagate': True
        },
        'django': {
            'handlers': ['file'],
            'level': 'INFO',
            'propagate': False,
        },
#        'log': {
#            'handlers': ['console'],
#            'level': 'INFO'
#        },
        'sqs': {
            'handlers': ['file'],
            'level': 'INFO'
        },
        'sys_stats': {
            'handlers': ['debug'],
            'level': 'DEBUG'
        },

    }
}

# Additional logging for sqs
#LOGGING['loggers']['sqs'] = {
#            'handlers': ['file'],
#            'level': 'INFO'
#        }

DEFAULT_AUTO_FIELD='django.db.models.AutoField'

# for testing
if "--disable-cache" in sys.argv:
    #USE_SQS_CACHING = False
    CACHES['default'] = {'BACKEND': 'django.core.cache.backends.dummy.DummyCache',}
    sys.argv.remove("--disable-cache")

