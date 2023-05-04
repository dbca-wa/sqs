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

CACHE_TIMEOUT_1_MINUTE = 60
CACHE_TIMEOUT_5_MINUTES = 60 * 5
CACHE_TIMEOUT_2_HOURS = 60 * 60 * 2
CACHE_TIMEOUT_24_HOURS = 60 * 60 * 24
CACHE_TIMEOUT = env('CACHE_TIMEOUT', CACHE_TIMEOUT_24_HOURS)

CHECK_APIURL_TOKEN = env('CHECK_APIURL_TOKEN', True)
CHECK_IP = env('CHECK_IP', True)

#USE_SQS_CACHING = env('USE_SQS_CACHING', True)

# Use 'epsg:4326' as projected coordinate system - 'epcg:4326' coordinate system is in meters (Then the buffer distance will be in meters)
CRS = env('CRS', 'epsg:4326')
CRS_CARTESIAN = env('CRS_CARTESIAN', 'epsg:4462')

TIME_ZONE = 'Australia/Perth'


if env('CONSOLE_EMAIL_BACKEND', False):
   EMAIL_BACKEND = 'django.core.mail.backends.console.EmailBackend'


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
    'dbca_utils.middleware.SSOLoginMiddleware',
    #'sqs.middleware.CacheControlMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware'
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

# Database
DATABASES = {
    # Defined in the DATABASE_URL env variable.
    'default': database.config(),
}

#CRON_CLASSES = [
#    'sqs.cron.OracleIntegrationCronJob',
#]

BASE_URL=env('BASE_URL')

if not os.path.exists(os.path.join(BASE_DIR, 'logs')):
    os.mkdir(os.path.join(BASE_DIR, 'logs'))
LOGGING = {
    'version': 1,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(message)s'
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
            'filename': os.path.join(BASE_DIR, 'logs', 'ledger.log'),
            'formatter': 'verbose',
            'maxBytes': 5242880
        },
    },
    'loggers': {
        '': {
            'handlers': ['file', 'console'],
            'level': env('LOG_CONSOLE_LEVEL', 'WARNING'),
            'propagate': True
        },
        'django': {
            'handlers': ['file'],
            'level': 'INFO',
            'propagate': False,
        },
        'log': {
            'handlers': ['file'],
            'level': 'INFO'
        },
        'sqs': {
            'handlers': ['file'],
            'level': 'INFO'
        },
    }
}

# Additional logging for sqs
LOGGING['loggers']['sqs'] = {
            'handlers': ['file'],
            'level': 'INFO'
        }
DEFAULT_AUTO_FIELD='django.db.models.AutoField'

# for testing
if "--disable-cache" in sys.argv:
    #USE_SQS_CACHING = False
    CACHES['default'] = {'BACKEND': 'django.core.cache.backends.dummy.DummyCache',}
    sys.argv.remove("--disable-cache")

