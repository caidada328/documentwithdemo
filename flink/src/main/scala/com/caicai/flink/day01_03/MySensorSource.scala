package com.atguiug.flink.day01_03

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class MySensorSource extends SourceFunction[SensorReading]{
  //定义一个flag,表示数据源是否正常运行
  var running =  true
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    //定义一个随机数发生器
    val rand = new Random()

    //定义10个传传感器的温度，并且在之前的温度基础上不断更新
    //首先生成10个传感器的初始温度
    var curTemps = 1.to(10).map(
      i =>("sensor_" + i,60 + rand.nextGaussian() * 20)//表示是一个正态分布的随机数
    )
    //无限循环，生成随机数据流
    while(running){
      curTemps = curTemps.map(
        data => (data._1,data._2 +rand.nextGaussian())
      )

      val curTs =System.currentTimeMillis()

      curTemps.foreach(
        data => ctx.collect(SensorReading(data._1,curTs.isInstanceOf[Long].asInstanceOf,data._2))
      )
      Thread.sleep(1000)
    }

  }

  override def cancel(): Unit ={
    running = false
  }
}
