package com.atguiug.flink.day04

import com.atguiug.flink.day01_03.SensorReading
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

//自定义一个断点式的  特定时间插入一个waterMark
class myAssigner2 (lateness:Long)extends AssignerWithPunctuatedWatermarks[SensorReading]{
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if (lastElement.id == "sensor_1") {
      new Watermark(extractedTimestamp - lateness)
    }
    else
    null
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = element.timestamp*1000L
}
