package PickSentence

import java.security.MessageDigest

import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object SparkVersion {

  val REGION_NUMBER = 98
  val TASK_NUMBER = 8000
  val NGRAM_COUNT_PARTITION = 500
  val HBASE_ZOOKEEPER_QUORUM = "ip-10-0-0-5,ip-10-0-0-13,ip-10-0-0-12," +
    "ip-10-0-0-11,ip-10-0-0-10,ip-10-0-0-8,ip-10-0-0-7,ip-10-0-0-4"

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("usage: \n" +
        "(0) corpus input file location\n" +
        "(1) sentence index output file\n" +
        "(2) ngram input file location\n" +
        "(3) join test result\n" +
        "(4) sentence score output file\n")
      return
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val in = sc.textFile(args(0))
    val sentenceIndex = in
        .map(str => str.substring(1,str.length -1))
        .map(_.split(",",2))
        .map(a => (a(1),a(0)))

    val NgramIndex = sentenceIndex.repartition(TASK_NUMBER)
      .map{case(k,v) => (k.split("""\W+"""), v)}
      .flatMap(createNgramInvertedIndex)
      .aggregateByKey(new mutable.HashSet[String]())(
        (aggr, value) => aggr += value ,
        (aggr1, aggr2) => aggr1 ++= aggr2
      ).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val ngramCount = sc.sequenceFile(args(2),classOf[Text],classOf[IntWritable])
      .map{case(k,v) => (k.toString, v.toString)}
      .repartition(NGRAM_COUNT_PARTITION)
      .filter(k => k._1.matches("[a-zA-z\\s]+"))

    val joinCountSentence = ngramCount.join(NgramIndex)

    val candidatesAndSentence = joinCountSentence
      .repartition(TASK_NUMBER)
      .flatMap(createCandidatesAndSentece)

    val hbase_rdd = candidatesAndSentence.map(a => ((a._1, "a", a._2._1), a._2._2 + "," + a._2._3.mkString(",")))
    val sortedRDD = hbase_rdd.map{ case (k, v)  => ((saltKey(k._1,REGION_NUMBER), k._2, k._3),v)}
    val partitionedRdd = sortedRDD
      .repartitionAndSortWithinPartitions(new SentencePartitioner(REGION_NUMBER))

    val cells = partitionedRdd.map(r => {
      val saltedRowKey = Bytes.toBytes(r._1._1)
      val cellData = Bytes.toBytes(r._2)
      val cell = new KeyValue(
        saltedRowKey,
        Bytes.toBytes(r._1._2), // column familily
        Bytes.toBytes(r._1._3), // column qualifier (i.e. cell name)
        cellData)
      (new ImmutableBytesWritable(saltedRowKey), cell)
    })

    val conf2 = HBaseConfiguration.create()
    conf2.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM)
    val job = new Job(conf2)
    val table = new HTable(conf2, "word_candidate")
    PatchedHFileOutputFormat2.configureIncrementalLoad(job, table)
    val conf3 = job.getConfiguration // important(!)
    partitionedRdd.saveAsNewAPIHadoopFile(
      args(3),
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[PatchedHFileOutputFormat2],
      conf3
    )

    joinCountSentence
      .flatMap(mergeScore)
      .reduceByKey(_+_)
      .coalesce(100)
      .saveAsTextFile(args(4))

  }

  def createCandidatesAndSentece(args: (String, (String, mutable.HashSet[String]))): IndexedSeq[(String, (String, String, mutable.HashSet[String]))] = {
    val ngram = args._1
    val count = args._2._1
    val sIds = args._2._2
    val ngramArray = ngram.split("\\s+")
    val keyLen = ngramArray.length
    (0 until keyLen).map(
      i => {
        val tmp = ngramArray.clone()
        val candidates = tmp(i)
        tmp(i) = "_"
        val queryKey = tmp.mkString(",")
        ((queryKey),(candidates, count, sIds))
      }
    )
  }

  def mergeScore(args: (String, (String, mutable.HashSet[String]))): mutable.HashSet[(String, Double)] = {
    var score : Double = 0
    val ngramCount = args._2._1
    val docIds = args._2._2
    try {
      score = 1.0 / ngramCount.toInt
    } catch {
      case _ : Exception =>  score = 0
    }
    docIds.map(a => (a,score))
  }

  def createNgramInvertedIndex(args :(Array[String], String)) : IndexedSeq[(String, String)] = {
    val words = args._1
    val uuid = args._2
    (2 to 5).flatMap(
      i => words.sliding(i)
        .filter(_.length==i)
        .map(_.mkString(" "))
        .map(a =>(a,uuid))
    )
  }

  def saltKey(key: String, modulus: Int) : String = {
    val keyString = key
    val saltAsInt = md5_7Hex(keyString) % modulus
    val charsInSalt = digitsRequired(modulus)
    ("%0" + charsInSalt + "d").format(saltAsInt) + ":" + keyString
  }

  def digitsRequired(modulus: Int) : Int = {
    (Math.log10(modulus-1)+1).asInstanceOf[Int]
  }

  def md5_7Hex(s: String) = {
    val first7HexVlue = MessageDigest
      .getInstance("MD5")
      .digest(s.getBytes)
      .map("%02X".format(_))
      .mkString
      .substring(0,7)
    Integer.parseInt(first7HexVlue,16)
  }

}
