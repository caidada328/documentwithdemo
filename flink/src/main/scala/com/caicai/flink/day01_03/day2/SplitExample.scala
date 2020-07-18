package com.atguiug.flink.day01_03.day2

import org.apache.flink.streaming.api.scala._
//过时，后续会将侧输出流 利用split分开在select查询
object SplitExample {
    def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      val inputStream = env.fromElements((1000,2),(1001,4),(999,1))
      val  splittedStream = inputStream.split(t => if(t._1 > 1000) Seq("large")else Seq("small"))

      val largeStream = splittedStream.select("large")
      val smallStream = splittedStream.select("small")
      print("输出大流")
      largeStream.print()
      print("输出小流")
      smallStream.print()
      env.execute("splitAndSelect job")
  }



}
