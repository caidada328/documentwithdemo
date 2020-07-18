package com.atguiug.flink.day01_03


import org.apache.flink.streaming.api.scala._


object SensorTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val stream1 = env
    ////      .fromCollection(List(  或者设置为fromElement为测试时候使用
    ////        SensorReading("sensor_1", 1547718199, 35.8),
    ////        SensorReading("sensor_6", 1547718201, 15.4),
    ////        SensorReading("sensor_7", 1547718202, 6.7),
    ////        SensorReading("sensor_10", 1547718205, 38.1)
    ////      ))

    //从文件读取
    val stream1:DataStream[String] = {
      env.readTextFile("C://Scalaa//flink//src//main//resources//a.txt")
    }
    //socket文本流
    val stream2 = env.socketTextStream("hadoop104",777)


    stream1.print("stream1:").setParallelism(1)

 env.execute("sourceTest")
  }
}
case class SensorReading(id:String,timestamp:Long,tempreture:Double)