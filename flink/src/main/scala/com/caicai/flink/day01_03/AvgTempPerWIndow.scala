package com.atguiug.flink.day01_03

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.functions.AggregateFunction


object AvgTempPerWIndow {
  def main(args: Array[String]): Unit = {

    val env =StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val source = env.addSource(new MySensorSource)

    val stream = source.map(r =>(r.id,r.tempreture))
      .keyBy(1).timeWindow(Time.seconds(5)).aggregate( new AggreagateFun)
      .print()

    env.execute("perwindowFunction")
  }
  class AggreagateFun extends AggregateFunction[(String,Double),(String,Double,Long),(String,Double)] {
    override def createAccumulator(): (String, Double, Long) = ("",0.0,0L)

    override def add(value: (String, Double), accumulator: (String, Double, Long)): (String, Double, Long) ={
      (value._1,value._2+accumulator._2,accumulator._3+1)
    }

    override def getResult(accumulator: (String, Double, Long)): (String, Double) = {
      (accumulator._1,accumulator._2/accumulator._3)
    }

    override def merge(a: (String, Double, Long), b: (String, Double, Long)): (String, Double, Long) = {
      (a._1,a._2+b._2,a._3+b._3)
    }
  }

}
