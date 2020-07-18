package com.atguiug.flink.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object streamwordcount {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    val host = params.get("host")

    val port = params.getInt("port")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val socketStream = env.socketTextStream(host,port)

    val dataStream = socketStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1).setParallelism(1)

    dataStream.print()

    env.execute("first Stream job")
  }

}
