package day2

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RichExample{
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val rawStream = env.fromElements(1,2,3)
        .flatMap(new MyFlatMapFunction)
        .print()

    env.execute("richwithliferutine")
  }

  class MyFlatMapFunction extends RichFlatMapFunction[Int,Int] {

    override def open(parameters: Configuration): Unit = {
      print("bravo start")
    }

//new MapStateDiscriptor
    override def setRuntimeContext(t: RuntimeContext): Unit = {
      t.getTaskName
    }

    override def flatMap(value: Int, out: Collector[Int]): Unit = {
      println("并行任务索引是：" + getRuntimeContext.getIndexOfThisSubtask)
      getRuntimeContext.getDistributedCache
      out.collect(value +  1)
    }


    override def close(): Unit = {
      print("happy ending")
    }
  }
}