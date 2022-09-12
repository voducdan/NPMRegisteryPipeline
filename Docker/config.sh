#!/bin/sh
for i in docker-npmregistry-airflow-worker-1 docker-npmregistry-airflow-webserver-1 docker-npmregistry-airflow-scheduler-1 docker-npmregistry-airflow-triggerer-1
do
  docker exec -u root $i chmod -R a+w /opt/airflow
  echo "Grant permission on $i"
done
