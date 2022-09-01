#!/usr/bin/env bash

# Move to the AIRFLOW HOME directory
cd $AIRFLOW_HOME

# Initiliaze the metadatabase
airflow initdb
# Initiliaze the webserver
airflow connections --add --conn_id 'postgres' --conn_type Postgres --conn_host 'postgres' --conn_login 'airflow' --conn_password 'airflow' --conn_schema 'brgroup'
airflow connections --add --conn_id 'ssh_spark_submit' --conn_type ssh --conn_host 'spark_submit' --conn_login 'test' --conn_password 'test'
exec airflow webserver  &> /dev/null &
# Initiliaze the scheduler
exec airflow scheduler