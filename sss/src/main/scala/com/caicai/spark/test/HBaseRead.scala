/*
package com.caicai.spark.test

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.mutable


object HBaseRead {
/*  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("HbaseRead").setMaster("local[*]")
    val sc = new SparkContext(sparkconf)

    val hbaseConf =HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper,quorum","hadoop104,hadoop105,hadoop106")
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"student")

    val rdd1 = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    val resultRDD = rdd1.map{
      case (iw, result) =>
        val map = mutable.Map[String,Any]()

        map += "rowKey" ->Bytes.toString(iw.get())
        val cells = result.listCells()
        import scala.collection.JavaConversions._
        for(cell <- cells){
          val key = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          map += key -> value
        }
        implicit  val df: DefaultFormats.type = org.json4s.DefaultFormats
        Serialization.write(map)
      case _ => throw new Exception("wrong infomation")
    }
  }*/

  /*def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("hbaseRead")
    val sc = new SparkContext(conf)
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","hadoop104,hadoop105,hadoop106")
    hbaseConf .set(TableInputFormat.INPUT_TABLE,"strudent")

    val rdd = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    import scala.collection.JavaConversions._

    val resultRDD = rdd.map{
      case (iw,result) =>
        val map = mutable.Map[String,Any]()
        map += "rowKey" -> iw.get()
        val cells = result.listCells()
        for(cell <- cells){
          val key = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          map += key -> value
        }
        implicit val df: DefaultFormats.type = org.json4s.DefaultFormats
        Serialization.write(map)
      case _=> throw  new IOException("please checkout your procedure")
    }
  }*/

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("hbaseRead")
    val sc = new SparkContext(conf)
    val hbaseConf =HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","hadoop104,hadoop105,hadoop106")
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"studnet")

    val rdd1 = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    import scala.collection.JavaConversions._
    val resultRdd = rdd1.map{
      case(iw,result) =>
        val map = mutable.HashMap[String,Any]()
        map += "rowKey" -> iw.get()
        val cells = result.listCells()
        for(cell <- cells){
          val key = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          map += key -> value
        }
        implicit val df: DefaultFormats.type = org.json4s.DefaultFormats
        Serialization.write(map)

      case _=> throw new Exception("wrong Information")

    }
    sc.stop()

  }
}
*/
