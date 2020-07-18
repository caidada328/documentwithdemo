package com.atguiug.flink.day01_03
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object WordCount {
   def main(args: Array[String]): Unit = {
     val env = StreamExecutionEnvironment.getExecutionEnvironment
     env.setParallelism(1)
     val stream = env.socketTextStream("localhost",9999)
   // val rawStream = stream.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
     env.execute()
  }
}
