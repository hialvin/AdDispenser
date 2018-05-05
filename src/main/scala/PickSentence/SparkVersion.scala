package PickSentence

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

object SparkVersion {

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
    val sentenceIndex = in.flatMap(_.split("\\.+")).
      map(_.trim).filter(x => x != null && x.length > 10)
      .zipWithUniqueId
    if (!args(1).equals("skip")){
      sentenceIndex.saveAsTextFile(args(1))
    }

    val NgramIndex = sentenceIndex.repartition(100)
      .map{case(k,v) => (k.split("""\W+"""), v)}
      .flatMap(createNgramInvertedIndex)
      .aggregateByKey(List[Any]())(
        (aggr, value) => aggr ::: (value :: Nil),
        (aggr1, aggr2) => aggr1 ::: aggr2
      )

    val ngramCount = sc.sequenceFile(args(2),classOf[Text],classOf[IntWritable])
      .map{case(k,v) => (k.toString, v.toString)}
      .filter(k => k._1.matches("[a-zA-z\\s]+"))

    val joinCount = ngramCount.join(NgramIndex)
    if (!args(3).equals("skip")) {
      joinCount.saveAsTextFile(args(3))
    }

    joinCount.flatMap(getScore)
      .reduceByKey(_+_)
      .saveAsTextFile(args(4))

  }
  def createNgramInvertedIndex(args :(Array[String], Long)) : List[(String, Long)] = {
    val words = args._1
    val uuid = args._2
    var res = ListBuffer[(String,Long)]()
    for (i <- 3 to 5) {
      val arr = new Array[String](i)
      for (j <- 0 to words.length - i) {
        for (k <- j until j + i) {
          arr(k - j) = words(k)
        }
        val ngram = arr.mkString(" ")
        val temp = (ngram, args._2)
        res += temp
      }
    }
    res.toList
  }

  def getScore(args: (String,(String,List[Any]))): List[(Any,Double)] ={
    val ngram = args._1
    val count = args._2._1
    var score : Double = 0
    try {
      score = 1.0 / count.toInt
    } catch {
      case _: Exception => 0
    }
    val sentenceList = args._2._2
    var res = ListBuffer[(Any,Double)]()
    for (sentence <- sentenceList) {
      val tmp = (sentence, score)
      res += tmp
    }
    res.toList
  }
}
