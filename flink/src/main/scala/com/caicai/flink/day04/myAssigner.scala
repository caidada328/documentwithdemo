package com.atguiug.flink.day04

import com.atguiug.flink.day01_03.SensorReading
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark

class myAssigner(lateness:Long) extends AssignerWithPeriodicWatermarks[SensorReading]{
  //隔一段时间获取一个wateramrk
  //需要两个参数 延迟时间，当前数据所有数据的最大时间戳


  var maxTs:Long = Long.MinValue+lateness
  override def getCurrentWatermark: Watermark = new  Watermark(maxTs - lateness)

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long ={
    maxTs = maxTs.max(element.timestamp*1000L)
    element.timestamp*1000L

  }
}


