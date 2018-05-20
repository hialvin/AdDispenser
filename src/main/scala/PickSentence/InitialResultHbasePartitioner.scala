package PickSentence
import org.apache.spark.Partitioner

class InitialResultHbasePartitioner(modulus: Int) extends Partitioner {

  val charsInSalt = digitsRequired(modulus)

  def getPartition(key: Any): Int = key match {
    case tup:(String, String, String) => tup._1.substring(0,charsInSalt).toInt
  }

  override def numPartitions: Int = modulus

  def digitsRequired(modulus: Int) : Int = {
    (Math.log10(modulus-1)+1).asInstanceOf[Int]
  }

}
