package redis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

object NgramToRedis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ngram to redis import file")
    val sc = new SparkContext(conf)
    val in = sc.sequenceFile("s3n://word-seeker-ingestion/*gram/part-*",classOf[Text],classOf[IntWritable])
    in.map{case(k,v) => (k.toString, v.toString)}
      .filter(k => k._1.matches("[a-zA-z\\s]+")).map(convertToRedisFormat)
      .saveAsTextFile("s3n://word-seeker-ingestion/redis-ingestion-file/")
  }
  def convertToRedisFormat(args:(String, String)):String = {
    val key = args._1
    val value = args._2
    "*3\r\n$3\r\nSET\r\n$"+ key.length + "\r\n" + key + "\r\n$"+ value.length+ "\r\n" + value+ "\r"
  }
}
