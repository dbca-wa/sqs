#!/bin/bash
  
# Start the first process
env > /etc/.cronenv
sed -i 's/\"/\\"/g' /etc/.cronenv

service cron start &
status=$?
if [ $status -ne 0  ]; then
      echo "Failed to start cron: $status"
        exit $status
    fi

    # Start the second process
    gunicorn sqs.wsgi --bind :8080 --config /app/gunicorn.ini
    status=$?
    if [ $status -ne 0  ]; then
          echo "Failed to start gunicorn: $status"
            exit $status
        fi
