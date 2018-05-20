package ingestion

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.spark.{SparkConf, SparkContext}

object NgramToS3 {

  val HDFS_PUBLIC_DNS = "hdfs://ec2-18-205-44-23.compute-1.amazonaws.com:9000"
  def main(args: Array[String]) {
    if (args.length == 0) {
      println("dude, i need at least one parameter")
      return
    }
    val curGram = args(0)
    val conf = new SparkConf().set("spark.hadoop.io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec,org.apache.hadoop.io.compress.BZip2Codec").setAppName(curGram + "gram ingestion")
    val sc = new SparkContext(conf)
    val in = sc.sequenceFile("s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/" + curGram + "gram/data",classOf[LongWritable],classOf[Text])
    //make data serializable
    val out = in.filter(a => a._2 != null)
                .map{a => a._2.toString.split("\\t+")}
                .filter(_.length > 2)
                .map(a => (a(0), a(2).toLong))
                .reduceByKey(_+_)
    out.saveAsSequenceFile(HDFS_PUBLIC_DNS + "/ngram/" + curGram + "-gram/")
    sc.stop()
  }

}

