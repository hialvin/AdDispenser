package redis

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object CandidatesToRedis {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("usage: (1)input file location, (2) output file location")
      return
    }
    val conf = new SparkConf().setAppName("candidates to redis import file")
    val sc = new SparkContext(conf)
    val in = sc.sequenceFile("s3n://word-seeker-ingestion/" + args(0),classOf[Text],classOf[IntWritable])
    val ngram = in.map{case(k,v) => (k.toString, v.toString)}
      .filter(k => k._1.matches("[a-zA-z\\s]+"))
      ngram.flatMap(generateCandidates)
      .map(printRedisImportFormat)
      .saveAsTextFile("s3n://word-seeker-ingestion/redis-ingestion-file/" + args(1))
  }

  def generateCandidates(entry : (String,String)): List[(String,(String,String))] ={
    val ngram = entry._1
    val cnt = entry._2
    var res = ListBuffer[(String,(String,String))]()
    if (ngram != null) {
      val ngramArray = ngram.split("\\s+")
      for (i <- 0 until ngramArray.length) {
        val temp = (getNewKey(ngramArray,i), (ngramArray(i), cnt))
        res += temp
      }
    }
    res.toList
  }

  def getNewKey(ngramArray : Array[String], index : Int) : String = {
    val newArray = ngramArray.clone
    newArray(index) = "_"
    newArray.mkString(",")
  }

  def printRedisImportFormat(args : (String, (String,String))): String = {
    val key = args._1
    val value = args._2._1
    val cnt = args._2._2
    val keyLen = key.length
    val valueLen = value.length
    val cntLen = cnt.length
    s"*4\r\n$$4\r\nzadd\r\n$$$keyLen\r\n$key\r\n$$$cntLen\r\n$cnt\r\n$$$valueLen\r\n$value\r"
  }

}
