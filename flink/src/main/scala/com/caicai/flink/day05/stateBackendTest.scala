package com.atguiug.flink.day05

import com.atguiug.flink.day01_03.SensorReading

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala._

object stateBackendTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //集群中的state.Backend 有配置 /conf/flink.conf.yml
//    env.setStateBackend(new MemoryStateBackend())
//    env.setStateBackend(new FsStateBackend("path"))
//    env.setStateBackend(new RocksDBStateBackend("",true))
    /*
    * <dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb_2.11</artifactId>
    <version>1.10.0</version>
</dependency>
    *
    * */




    val inputStream = env.socketTextStream("hadoop104",7777)
    val dataStream = inputStream.map(
      data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })

  }
}
