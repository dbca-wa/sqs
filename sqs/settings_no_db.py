from sqs.settings import *

# Test runner with no database creation
#TEST_RUNNER = 'tests.no_db_test_runner.NoDbTestRunner'

#DATABASES['default']['NAME']='_test_sqs_dev'
DATABASES = {
    'default': {
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'NAME': '_test_sqs_dev__jm',
        'USER': 'dev',
        'PASSWORD': 'dev123',
        'HOST': 'localhost',
        'PORT': '5432',
    }
}
