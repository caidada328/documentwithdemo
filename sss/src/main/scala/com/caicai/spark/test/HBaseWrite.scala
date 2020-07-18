/*
package com.caicai.spark.test

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}


object HBaseWrite {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hbaseWrite").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","hadoop104,hadoop105,hadoop016")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,"student")
    val job =Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])

    val intialRDD =
      sc.parallelize(List(("2000","apple","11"),("2001","banana","12"),("2002","orange","13")))

    val hbaseRDD = intialRDD.map{
      case (rk,name,age) =>
        val rowkey = new ImmutableBytesWritable()
        rowkey.set(Bytes.toBytes(rk))
        val put = new Put(Bytes.toBytes(rk))
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(name))
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(age))
        (rowkey,put)
    }
    hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
    sc.stop()
  }

}
*/
