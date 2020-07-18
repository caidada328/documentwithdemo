package com.atguiug.flink.day01_03.day2

import org.apache.flink.streaming.api.scala._

object SourceFromCustomDataSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env .addSource(new SensorSource)
      // 添加数据源


    stream.print()

    env.execute()
  }
}