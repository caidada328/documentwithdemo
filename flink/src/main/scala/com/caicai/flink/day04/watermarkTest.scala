package com.atguiug.flink.day04

import com.atguiug.flink.day01_03.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object windowTesst {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置waterMark的插入间隔
    env.getConfig.setAutoWatermarkInterval(300L)

    val inputStream = env.readTextFile("C:\\Scalaa\\flink\\src\\main\\resources\\source.txt")
    val dataStream = inputStream.map(
      data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toInt,dataArray(2).toDouble)
      })
      //这个Time.seconds(10)就是手设延时时间
      //如果不想使用这个写死的延迟时间，可以重写BoundedOutOfOrdernessTimestampExtractor，基于实际情况去重写
      //需要使用到机器学习了
      //这个是系统已经帮我们写好的一个实现类BoundedOutOfOrdernessTimestampExtractor
    /* .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(10)) {
       override def extractTimestamp(ele:SensorReading): Long = ele.timestamp.toLong
     } )*/
      .assignTimestampsAndWatermarks(new myAssigner(1000L))

    val resultStream = dataStream
        .keyBy("id")

        .timeWindow(Time.seconds(5),Time.seconds(1))
        .allowedLateness(Time.seconds(4))//允许延迟的时间
      //如果还有延迟的数据，就是说这个waterMark数据的延迟时间设置的不合适，那么就会导致有数据会滞留，
      // 至于老师说的正态分布，我觉得不对，但是应该是类似于正态分布，正态分布的定义域可是（+∞，-∞）
        .sideOutputLateData(new OutputTag[SensorReading]("hahah"))


   // val a = resultStream.getsideOutput(new OutputTag[SensorReading]("hahah")
    println(resultStream)

    env.execute()
  }
}
