package redis

import com.redislabs.provider.redis._
import org.apache.spark.{SparkConf, SparkContext}

object TestConnection2 {
  def main(args: Array[String]): Unit = {
    val redisServerDnsAddress = "REDIS_HOSTNAME"
    val redisPortNumber = 6379
    val conf = new SparkConf().setAppName("ngram to redis import file")
      .set("redis.host","ec2-18-205-229-110.compute-1.amazonaws.com")
      .set("redis.port", "6379")
      .set("redis.auth","alvin@insight")
    val sc = new SparkContext(conf)

    val keysRDD = sc.fromRedisKeyPattern("foo*", 5)
    keysRDD.collect()

  }
}