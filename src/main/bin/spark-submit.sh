#!/bin/bash

#yyyyMMdd
DAY=$1

APP_NAME=hdfs-to-tdengine
APP_VERSION=v1.0
APP_JAR=../lib/demo-spark-hdfs-to-tdengine-1.0.jar
MAIN_CLASS=sunyu.demo.Main
FIX=fix${DAY}

APP_ID=`yarn application -list |grep ${APP_NAME} |awk '{print $1}'`

if [ "${APP_ID}" != "" ] ; then
  echo `yarn application -kill ${APP_ID}`
fi

echo "Start Spark Application ${APP_NAME}${APP_VERSION} ...."

for jar in ../lib/*.jar
do
  if [ ${jar} != ${APP_JAR} ] ; then
    LIBJARS=${jar},${LIBJARS}
  fi
done

for file in ../resources/*; do
  if [ -f ${file} ]; then
    RESOURCES_FILES=${file},${RESOURCES_FILES}
  fi
done

spark-submit \
  --class ${MAIN_CLASS} \
  --master yarn \
  --deploy-mode cluster \
  --jars ${LIBJARS} \
  --files ${RESOURCES_FILES} \
  --conf spark.app.name=${APP_NAME}_${APP_VERSION}_${FIX} \
  --conf spark.driver.cores=1 \
  --conf spark.driver.memory=2g \
  --conf spark.driver.maxResultSize=0 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.instances=5 \
  --conf spark.executor.memory=8g \
  --conf spark.executor.extraJavaOptions="-XX:+UseG1GC" \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.scheduler.mode=FIFO \
  --conf spark.streaming.concurrentJobs=1 \
  --conf spark.streaming.backpressure.enabled=false \
  --conf spark.streaming.kafka.maxRatePerPartition=100000 \
  --conf spark.streaming.stopGracefullyOnShutdown=true \
  $APP_JAR \
  /spark/farm_can/2025/03/25/part-000011742958000000
  # hdfsPath
