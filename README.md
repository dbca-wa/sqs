# Spatial Query Service

# Install SQS Project
```
 cd /var/www
 git clone https://github.com/dbca-wa/sqs.git
 cd sqs

 virtualenv venv
 . venv/bin/activate

 pip install -r requirements.txt 
```
     
## Add in .env
```
DATABASE_URL="postgis://test:my_passwd@localhost:5432/sqs_dev"
 
DEBUG=True
DATABASE_URL="postgis://test:my_passwd@localhost:5432/sqs_dev"
SYSTEM_GROUPS=['SQS Admin']
SITE_PREFIX='sqs-dev'
SITE_DOMAIN='dbca.wa.gov.au'
SECRET_KEY='SECRET_KEY_YO'
PRODUCTION_EMAIL=False
NOTIFICATION_EMAIL='firstname.lastname@dbca.wa.gov.au'
CRON_NOTIFICATION_EMAIL='[firstname.lastname]'
NON_PROD_EMAIL='firstname.lastname@dbca.wa.gov.au'
EMAIL_INSTANCE='DEV'
EMAIL_HOST='smtp.corporateict.domain'
DJANGO_HTTPS=True
DEFAULT_FROM_EMAIL='no-reply@dbca.wa.gov.au'
ALLOWED_HOSTS=['*']
CONSOLE_EMAIL_BACKEND=True
```


## Add in .env
```
go to http://localhost:8000/admin
Login with the above credentials (email: firstname.lastname@dbca.wa.gov.au, pw: my_password)
From th Admin view 
     1. create group 'SQS Admin' (in Group section)
     2. in API sections
         a. click Add API
         b. System Name: SQS
            System id:   0111 (some aritrary number for now)
            Allowed ips: 127.0.0.1/32
            Active:      active
         c. Click save
```


