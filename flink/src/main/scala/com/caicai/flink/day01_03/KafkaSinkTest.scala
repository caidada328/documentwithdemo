package com.atguiug.flink.day01_03

import org.apache.flink.api.common.serialization.{BulkWriter, Encoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path

import org.apache.flink.runtime.clusterframework.overlays.HadoopUserOverlay
import org.apache.flink.runtime.util.HadoopUtils
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("C:\\Scalaa\\flink\\src\\main\\resources\\source.txt")
    val dataStream = inputStream.map(
      data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble).toString
      }
    )
//    dataStream.addSink(new StreamingFileSink[String](StreamingFileSink.BucketsBuilder,1000)

//    一个个写入到本地，测试使用
 /*   dataStream.addSink(
      StreamingFileSink.forRowFormat(
      new Path("C:\\Scalaa\\flink\\src\\main\\resources\\out.txt"),
      new SimpleStringSchema[String]("UTF-8")).build())*/
 //批次写入

   /* val hadoopconf = HadoopUtils.getHadoopConfiguration()
    val sink = StreamingFileSink.forBulkFormat(new Path ("C:\\Scalaa\\flink\\\\src\\main\\resources\\out.txt"),
      new SequenceFileWriterFactory(hadoopconf,LongWritable.class,Text.class)).build)
    dataStream.addSink(sink)*/
  //那么如何写到外部的框架呢
    //写入到kafka
    dataStream.addSink(new FlinkKafkaProducer011[String]("hadoop104:9092","flink2Kafka_sink",new SimpleStringSchema()))
    env.execute("翻车了，老师")
  }

}
