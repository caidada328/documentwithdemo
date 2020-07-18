package com.atguiug.flink2.day01
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object wordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
   //socket数据源
    val stream = env.socketTextStream("hadoop104",9999)

    //wordcount具体操作
    val outputStream = stream.flatMap(_.split("\\W+"))
      .map(s=>(s,1))
      .keyBy(0)//安第一个位置分组
      .timeWindow(Time.seconds(5))
      .sum(1)

    println(outputStream)
    //不要忘记执行
    env.execute("Elements Sum Job")
  }

}
