package ingestion

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import util.control.Breaks._


object NgramToS3 {
  def combine_func(iter: Iterator[String]) : Iterator[(String,Int)] = {
    var res = ListBuffer[(String,Int)]()
    var pre_word = ""
    var total_cnt = 0
    var cur_word = ""
    while (iter.hasNext) {
      val cur_string = iter.next
      breakable{
        if (cur_string == null) {
          break
        }
        val cur_words = cur_string.split("\\t+")
        if (cur_words.length < 2) {
          break
        }
        val cur_word = cur_words(0)
        val cur_cnt = cur_words(2).toInt
        if (cur_word.equals(pre_word)){
          total_cnt += cur_cnt
        } else {
          if (pre_word.length() > 0) {
            val tmp = (cur_word, total_cnt)
            res += tmp
          }
          pre_word = cur_word
          total_cnt = 0
        }
      }
    }
    res += ((cur_word, total_cnt))
    res.iterator
  }

  def main(args: Array[String]) {

    if (args.length == 0) {
      println("dude, i need at least one parameter")
      return
    }
    val curGram = args(0)
    val conf = new SparkConf().set("spark.hadoop.io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec,org.apache.hadoop.io.compress.BZip2Codec").setAppName(curGram + "gram ingestion")

    val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
    val sc = new SparkContext(conf)
    println(sc.getConf.get("spark.hadoop.io.compression.codecs"))
    val in = sc.sequenceFile("s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/" + curGram + "gram/data",classOf[LongWritable],classOf[Text])
    //make data serializable
    val out = in.map{case(_,v) => v.toString}
      .mapPartitions(combine_func).repartition(64)
    out.saveAsSequenceFile("s3n://word-seeker-ingestion/" + curGram + "-gram/")
//    out.saveAsTextFile("s3n://word-seeker-ingestion/1-gram-text/")
//Some(classOf[GzipCodec])
    //.sample(false,12345678)
    sc.stop()
  }
}

