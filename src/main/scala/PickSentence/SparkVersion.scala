package PickSentence

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.{IntWritable, Text}
import scala.collection.mutable

object SparkVersion {

  val REGION_NUMBER = 50
  val TASK_NUMBER = 1000

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
    val conf = new SparkConf().setAppName("calculate sentence score")
    val sc = new SparkContext(conf)
    val in = sc.textFile(args(0))
    val sentenceIndex = in.map(_.split(","))
      .map(a => (a(0),a(1).toLong))
    val NgramIndex = sentenceIndex.repartition(TASK_NUMBER)
      .map{case(k,v) => (k.split("""\W+"""), v)}
      .flatMap(createNgramInvertedIndex)
      .aggregateByKey(new mutable.HashSet[Long]())(
        (aggr, value) => aggr += value ,
        (aggr1, aggr2) => aggr1 ++= aggr2
      )
    NgramIndex.saveAsTextFile(args(1))

    val ngramCount = sc.sequenceFile(args(2),classOf[Text],classOf[IntWritable])
      .map{case(k,v) => (k.toString, v.toString)}
      .filter(k => k._1.matches("[a-zA-z\\s]+"))

    val joinCountSentence = ngramCount.join(NgramIndex)

    joinCountSentence
      .repartition(TASK_NUMBER)
      .flatMap(createCandidatesAndSentece)
      .aggregateByKey(new mutable.HashSet[(String, String, mutable.HashSet[Long])]())(
        (aggr, value) => aggr += value ,
        (aggr1, aggr2) => aggr1 ++= aggr2
      )

    if (!args(3).equals("skip")) {
      joinCountSentence.saveAsTextFile(args(3))
    }

    joinCountSentence
      .repartition(TASK_NUMBER)
      .flatMap(mergeScore)
      .reduceByKey(_+_)
      .saveAsTextFile(args(4))

  }

  def createCandidatesAndSentece(args: (String, (String, mutable.HashSet[Long]))): IndexedSeq[(String, (String, String, mutable.HashSet[Long]))] = {
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

  def mergeScore(args: (String, (String, mutable.HashSet[Long]))): mutable.HashSet[(Long, Double)] = {
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

  def createNgramInvertedIndex(args :(Array[String], Long)) : IndexedSeq[(String, Long)] = {
    val words = args._1
    val uuid = args._2
    (2 to 5).flatMap(
      i => words.sliding(i)
        .filter(_.length==i)
        .map(_.mkString(","))
        .map(a =>(a,uuid))
    )
  }

}
