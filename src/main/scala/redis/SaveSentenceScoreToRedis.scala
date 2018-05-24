package redis

import com.redislabs.provider.redis._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.spark.HBaseContext


object SaveSentenceScoreToRedis {
  def main(args: Array[String]): Unit = {

    val REDIS_HOSTNAME = "18.205.207.110"
    val REDIS_PORT = "31001"

    val conf = new SparkConf().setAppName("save sentence score to redis")
      .set("redis.host",REDIS_HOSTNAME)
      .set("redis.port", REDIS_PORT)
    val sc = new SparkContext(conf)

    val keyValueRdd = sc.parallelize(1 to 9).map(a => (a.toString, "abcded"))
    sc.toRedisKV(keyValueRdd)
  }
}
