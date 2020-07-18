package com.atguiug.flink.day01_03



import org.apache.flink.api.common.time._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
object windowTesst {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("C:\\Scalaa\\flink\\src\\main\\resources\\source.txt")
    val dataStream = inputStream.map(
      data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toInt,dataArray(2).toDouble).toString
      })
   val result = dataStream
     .keyBy("id")
    // .windowAll(EventTimeSessionWindows.withGap(Time.minutes(1)))  会话窗口
     //.window(TumblingProcessingTimeWindows.of(Time.days(1),Time.hours(-8))) 底层滚动窗口的设置
//     .timeWindow(Time.seconds(3),Time.seconds(2))  滑动窗口
//     .countWindow(10,2) 计数窗口

  }
}
