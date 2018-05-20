package PickSentence

import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import java.security.MessageDigest

object SentenceIndexInHBase {

  val REGION_NUMBER = 70
  val HBASE_ZOOKEEPER_QUORUM = "ip-10-0-0-52,ip-10-0-0-48,ip-10-0-0-54,ip-10-0-0-37," +
    "ip-10-0-0-40,ip-10-0-0-55,ip-10-0-0-42,ip-10-0-0-56,ip-10-0-0-62"

  val HTABLE_NAME = "sentence_index"

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("usage: (1)input file location " +
        "(2) sentence index output file location " +
        "(3) HBase file location on HDFS")
      return
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val in = sc.textFile(args(0))

    val sentenceIndex = in.flatMap(_.split("\\.+")).
      map(_.trim).filter(x => x != null && x.length > 50)
      .zipWithUniqueId
    val partitionedSentenceRdd = sentenceIndex
      .map { case (k, v) => (saltKey(v, REGION_NUMBER), k) }
      .repartitionAndSortWithinPartitions(new SentencePartitioner(REGION_NUMBER))
    partitionedSentenceRdd.saveAsTextFile(args(1))

    val cells = partitionedSentenceRdd.map(r => {
      val saltedRowKey = Bytes.toBytes(r._1)
      val cellData = Bytes.toBytes(r._2)
      val cell = new KeyValue(
        saltedRowKey,
        Bytes.toBytes("a"), // column familily
        Bytes.toBytes("e"), // column qualifier (i.e. cell name)
        cellData)
      (new ImmutableBytesWritable(saltedRowKey), cell)
    })

    val conf2 = HBaseConfiguration.create()
    conf2.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM)
    val job = new Job(conf2)
    val table = new HTable(conf2, HTABLE_NAME)
    PatchedHFileOutputFormat2.configureIncrementalLoad(job, table)
    val conf3 = job.getConfiguration // important(!)
    cells.saveAsNewAPIHadoopFile(
      args(2),
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[PatchedHFileOutputFormat2],
      conf3
    )
  }

  def saltKey(key: Long, modulus: Int) : String = {
    val keyString = key.toString
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
