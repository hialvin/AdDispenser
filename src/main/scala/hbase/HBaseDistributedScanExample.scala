package hbase

import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Scan
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase
/**
  * This is a simple example of scanning records from HBase
  * with the hbaseRDD function.
  */
object HBaseDistributedScanExample {

  val HBASE_ZOOKEEPER_QUORUM = "ip-10-0-0-10,ip-10-0-0-13,ip-10-0-0-4"
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("GenerateGraphs {tableName}")
      return
    }

    val tableName = args(0)

    val sparkConf = new SparkConf().setAppName("HBaseDistributedScanExample " + tableName )
    val sc = new SparkContext(sparkConf)

    try {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM)

      val hbaseContext = new HBaseContext(sc, conf)

      val scan = new Scan()
      scan.setCaching(100)

      val getRdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan)

      getRdd.foreach(v => println(Bytes.toString(v._1.get())))

      println("Length: " + getRdd.map(r => r._1.copyBytes()).collect().length);

      //.collect().foreach(v => println(Bytes.toString(v._1.get())))
    } finally {
      sc.stop()
    }
  }

}
