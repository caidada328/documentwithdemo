package com.atguiug.flink.wordcount

import org.apache.flink.api.scala._

object batchwordcount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "C:\\Scalaa\\flink\\src\\main\\resources\\a.txt"

    val inputDs = env.readTextFile(inputPath)


    val wordcountDS = inputDs
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)


    wordcountDS.print()


  }

}
