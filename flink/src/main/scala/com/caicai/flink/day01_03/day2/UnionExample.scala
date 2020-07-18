package com.atguiug.flink.day01_03.day2

import org.apache.flink.streaming.api.scala._
//union不去重，可连接多个流，只能连接相同的流。
object UnionExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val parisStream: DataStream[SensorReading] =env.addSource(new SensorSource).filter(_.id.equals("sensor_1"))
    val tokyoStream: DataStream[SensorReading] = env.addSource(new SensorSource).filter(_.id.equals("sensor_2"))
    val rioStream: DataStream[SensorReading] = env.addSource(new SensorSource).filter(_.id.equals("sensor_3"))
    val newyorkSteam = env.fromElements("a",1,"bbb")
    val connectStream = parisStream.connect(newyorkSteam)

    val allCitiesStream: DataStream[SensorReading] = parisStream.union(tokyoStream,rioStream)
    allCitiesStream.print()
    env.execute("union task")
  }

}
