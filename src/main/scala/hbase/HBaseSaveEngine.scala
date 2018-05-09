package hbase

import java.security.MessageDigest

import PickSentence.{PatchedHFileOutputFormat2, SentencePartitioner}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

class HBaseSaveEngine(regionNumber: Int, HTableName: String ,HBaseZookeeperQuorum: String, outputLocation: String) {

  def save(rdd: RDD[((String,String,String),String)], isSorted: Boolean): Unit = {

    var sortedRDD:RDD[((String,String,String),String)] = null
    if (!isSorted) {
      sortedRDD = rdd.map{ case (k, v)  => ((saltKey(k._1,regionNumber), k._2, k._3),v)}
    } else {
      sortedRDD = rdd
    }
    val partitionedRdd = sortedRDD
      .repartitionAndSortWithinPartitions(new SentencePartitioner(regionNumber))

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
    conf2.set("hbase.zookeeper.quorum", HBaseZookeeperQuorum)
    val job = new Job(conf2)
    val table = new HTable(conf2, HTableName)
    PatchedHFileOutputFormat2.configureIncrementalLoad(job, table)
    val conf3 = job.getConfiguration // important(!)
    rdd.saveAsNewAPIHadoopFile(
      outputLocation,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[PatchedHFileOutputFormat2],
      conf3
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
