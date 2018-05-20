#!/usr/bin/env bash
spark-submit \
--name "initial result sample data" \
--master spark://ip-10-0-0-52.ec2.internal:7077 \
--executor-memory 12G \
--driver-memory 6G \
--executor-cores 2 \
--conf spark.default.parallelism=200 \
--conf "spark.executor.extraJavaOptions=-XX:+UseCompressedOops -XX:+UseG1GC" \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.shuffle.service.enabled=true \
--conf spark.network.timeout=600s \
--conf spark.reducer.maxReqsInFlight=1 \
--conf spark.shuffle.io.retryWait=60s \
--conf spark.shuffle.io.maxRetries=5 \
--class PickSentence.InitialResult \
--jars $(echo /usr/local/hbase/lib/*.jar | tr ' ' ',') \
~/wordseeker_2.11-0.1.jar \
200 \
hdfs://ec2-18-205-44-23.compute-1.amazonaws.com:9000/CompleteSentenceIndex \
hdfs://ec2-18-205-44-23.compute-1.amazonaws.com:9000/ngram/3-gram/* \
hdfs://ec2-18-205-44-23.compute-1.amazonaws.com:9000/SampleJoinCountSentenceToHbase \
hdfs://ec2-18-205-44-23.compute-1.amazonaws.com:9000/SampleSentenceScore/

