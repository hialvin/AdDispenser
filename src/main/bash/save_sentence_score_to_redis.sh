#!/usr/bin/env bash
spark-submit \
--name "Main" \
--master spark://ip-10-0-0-10.ec2.internal:7077 \
--executor-memory 12G \
--driver-memory 6G \
--executor-cores 2 \
--class redis.SaveSentenceScoreToRedis \
--jars $(echo /usr/local/hbase/lib/*.jar | tr ' ' ','),/home/ubuntu/spark-redis/target/spark-redis-0.3.2-jar-with-dependencies.jar \
~/wordseeker_2.11-0.1.jar

