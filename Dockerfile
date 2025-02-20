# Prepare the base environment.
FROM ubuntu:24.04 AS builder_base_sqs
MAINTAINER asi@dbca.wa.gov.au
ENV DEBIAN_FRONTEND=noninteractive
ENV DEBUG=True
ENV TZ=Australia/Perth
ENV EMAIL_HOST="smtp.corporateict.domain"
ENV DEFAULT_FROM_EMAIL='no-reply@dbca.wa.gov.au'
ENV NOTIFICATION_EMAIL='jawaid.mushtaq@dbca.wa.gov.au'
ENV NON_PROD_EMAIL='jawaid.mushtaq@dbca.wa.gov.au'
ENV PRODUCTION_EMAIL=False
ENV EMAIL_INSTANCE='DEV'
ENV SECRET_KEY="ThisisNotRealKey"
ENV SITE_PREFIX='sqs-dev'
ENV SITE_DOMAIN='dbca.wa.gov.au'
ENV OSCAR_SHOP_NAME='Parks & Wildlife'
ENV BPAY_ALLOWED=False

RUN apt-get clean
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install --no-install-recommends -y wget git libmagic-dev gcc binutils libproj-dev build-essential python3 python3-setuptools python3-dev python3-pip tzdata libreoffice cron rsyslog 
RUN apt-get install --no-install-recommends -y libpq-dev patch
RUN apt-get install --no-install-recommends -y postgresql-client mtr
RUN apt-get install --no-install-recommends -y sqlite3 vim postgresql-client ssh htop
RUN apt-get install --no-install-recommends -y graphviz libgraphviz-dev pkg-config run-one virtualenv software-properties-common

# Install GDAL
RUN add-apt-repository ppa:ubuntugis/ubuntugis-unstable
RUN apt update
RUN apt-get install --no-install-recommends -y gdal-bin libgdal-dev python3-gdal

RUN groupadd -g 5000 oim
RUN useradd -g 5000 -u 5000 oim -s /bin/bash -d /app
RUN usermod -a -G sudo oim
RUN mkdir /app
RUN chown -R oim.oim /app

COPY timezone /etc/timezone
ENV TZ=Australia/Perth
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY startup.sh /
RUN chmod 755 /startup.sh

# kubernetes health checks script
RUN wget https://raw.githubusercontent.com/dbca-wa/wagov_utils/main/wagov_utils/bin/health_check.sh -O /bin/health_check.sh
RUN chmod 755 /bin/health_check.sh

# add python cron
RUN wget https://raw.githubusercontent.com/dbca-wa/wagov_utils/main/wagov_utils/bin-python/scheduler/scheduler.py -O /bin/scheduler.py
RUN chmod 755 /bin/scheduler.py




# Install Python libs from requirements.txt.
FROM builder_base_sqs as python_libs_sqs
WORKDIR /app
USER oim
#RUN virtualenv -p python3.11 /app/venv
RUN virtualenv /app/venv
ENV PATH=/app/venv/bin:$PATH
COPY --chown=oim:oim requirements.txt ./

#COPY requirements.txt ./
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt 
# Update the Django <1.11 bug in django/contrib/gis/geos/libgeos.py
# Reference: https://stackoverflow.com/questions/18643998/geodjango-geosexception-error
#&& sed -i -e "s/ver = geos_version().decode()/ver = geos_version().decode().split(' ')[0]/" /usr/local/lib/python3.6/dist-packages/django/contrib/gis/geos/libgeos.py \
#  && rm -rf /var/lib/{apt,dpkg,cache,log}/ /tmp/* /var/tmp/*

# Install the project (ensure that frontend projects have been built prior to this step).
FROM python_libs_sqs

#COPY libgeos.py.patch /app/
#RUN patch /usr/local/lib/python3.8/dist-packages/django/contrib/gis/geos/libgeos.py /app/libgeos.py.patch
#RUN rm /app/libgeos.py.patch

#COPY cron /etc/cron.d/dockercron
COPY --chown=oim:oim gunicorn.ini manage.py ./
RUN touch /app/.env
COPY --chown=oim:oim .git ./.git
COPY --chown=oim:oim python-cron python-cron
COPY --chown=oim:oim sqs ./sqs
#RUN mkdir /app/sqs/cache/
#RUN chmod 777 /app/sqs/cache/
RUN whereis pip
RUN whereis python
RUN ls -al /app/venv/
RUN python manage.py collectstatic --noinput
#RUN apt-get install --no-install-recommends -y python-pil
EXPOSE 8080
HEALTHCHECK --interval=1m --timeout=5s --start-period=10s --retries=3 CMD ["wget", "-q", "-O", "-", "http://localhost:8080/"]
CMD ["/startup.sh"]
#CMD ["gunicorn", "parkstay.wsgi", "--bind", ":8080", "--config", "gunicorn.ini"]

