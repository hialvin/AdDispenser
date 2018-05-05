package PickSentence

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

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

    val NgramIndex = sentenceIndex.repartition(50)
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

    joinCount.flatMap(mergeScore)
      .reduceByKey(_+_)
      .saveAsTextFile(args(4))

  }

  def mergeScore(args: (String, (String, List[Any]))): List[(Any, Double)] = {
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

  def createNgramInvertedIndex(args :(Array[String], Long)) : Iterator[(String, Long)] = {
    val words = args._1
    val uuid = args._2
    words.sliding(3)
                .filter(_.length==3)
                .map(_.mkString(","))
                .map(a =>(a,uuid)
    )
  }

}
