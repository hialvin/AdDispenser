package PickSentence

import org.apache.spark.Partitioner

class SentencePartitioner(modulus: Int) extends Partitioner {

  val charsInSalt = digitsRequired(modulus)

  def getPartition(key: Any): Int = key match {
    case key:String => key.substring(0,charsInSalt).toInt
  }

  override def numPartitions: Int = modulus

  def digitsRequired(modulus: Int) : Int = {
    (Math.log10(modulus-1)+1).asInstanceOf[Int]
  }

}
