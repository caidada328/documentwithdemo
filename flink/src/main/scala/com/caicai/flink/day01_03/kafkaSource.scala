package com.atguiug.flink.day01_03

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.core.memory.MemorySegment
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
/*
* slotsharingGroup
* 在同一个共享组中的算子才能使用这个组，使用这个就可能出现本来设置平行度最大为2，但是会出现一共使用大于2的slot
* 允许不同的算子在不同的slot中
* 一般是因为默认所有算子使用同一个slotsharingGroup中
*
* */
object kafkaSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//MemorySegment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop104:9092")
    properties.setProperty("group.id", "consumer_group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

//表示kafka的版本号 011
    val stream1 = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))

    stream1.print("stream1:").setParallelism(1)
    //NetworkBuffer
    env.execute()
  }

}
