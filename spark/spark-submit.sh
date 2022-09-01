#!/usr/bin/env bash

cd /opt/bitnami/spark/bin
#removing checkpoint from last execution. test_only!
# rm -rf ../work/resources/cp
export PATH=$PATH:/opt/bitnami/python/bin:/opt/bitnami/java/bin:/opt/bitnami/spark/bin:/opt/bitnami/spark/sbin:/opt/bitnami/common/bin
#submiting spark jar file
echo 'running spark app'
spark-submit \
--packages org.postgresql:postgresql:42.4.1 \
--deploy-mode client  \
--master spark://spark:7077 \
--name  BRGroup  \
../work/brgroup/brgroup_2.11/0.1/brgroup_2.11-0.1-assembly.jar ../work/resources/config/log4j.properties ../work/resources/config/config_local.ini docker 