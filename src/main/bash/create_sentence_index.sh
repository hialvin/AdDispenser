#!/usr/bin/env bash

spark-submit \
--name "create sentence index complete data" \
--master spark://ip-10-0-0-52.ec2.internal:7077 \
--executor-memory 12G \
--driver-memory 4G \
--executor-cores 2 \
--conf spark.default.parallelism=100 \
--conf spark.hadoop.validateOutputSpecs=false \
--jars $(echo /usr/local/hbase/lib/*.jar | tr ' ' ',') \
--class PickSentence.SentenceIndexInHBase \
~/wordseeker_2.11-0.1.jar \
hdfs://ec2-18-205-44-23.compute-1.amazonaws.com:9000/umbc_corpus/sample400M \
hdfs://ec2-18-205-44-23.compute-1.amazonaws.com:9000/sample400MSentenceIndex \
hdfs://ec2-18-205-44-23.compute-1.amazonaws.com:9000/sample400MSentenceIndex \
