package com.atguiug.flink.day05

import com.atguiug.flink.day01_03.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
object sideOutput {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val inputStream = env.readTextFile("C:\\Scalaa\\flink\\src\\main\\resources\\source.txt")
    val dataStream = inputStream.map(
      data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })

    val highTempStream = dataStream.process( new SplitTempProcess(30))

    val lowTempStream = highTempStream.getSideOutput(new OutputTag[(String,Double,Long)]("low-temp"))

    highTempStream.print("high-temp")
    lowTempStream.print("low-temp")

  env.execute()
  }
}

class SplitTempProcess(threshold:Int) extends ProcessFunction[SensorReading,SensorReading]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    //判断当前的温度值大于阈值放在主流，别的放在侧流中

    if(value.tempreture > threshold)
      out.collect(value)
    else{
      ctx.output(new OutputTag[(String,Double,Long)]("low-temp"),(value.id,value.tempreture,value.timestamp) )
    }
  }
}
