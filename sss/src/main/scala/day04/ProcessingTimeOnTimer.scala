package day04

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//每一个key配合一个时间戳只能注册一个定时器，也就是说一个key可以注册多个 定时器

object ProcessingTimeOnTimer {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("hadoop104",9999)
      .map(line =>{
        val array =line.split("")
        (array(0),array(1))

      })
      .keyBy(_._1)
      .process(new MyKeyedProcess)
    stream.print()
    env.execute("KeyedProcessFunctoin")

  }
  class MyKeyedProcess extends KeyedProcessFunction[String,(String,String),String] {
    override def processElement(value: (String, String), ctx: KeyedProcessFunction[String, (String, String), String]#Context,
                                out: Collector[String]): Unit = {
      var curTime = ctx.timerService().currentProcessingTime()
      ctx.timerService().registerProcessingTimeTimer(curTime + 10 * 1000L)
      out.collect("当前水位线是：" + ctx.timerService().currentWatermark())
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String), String]#OnTimerContext, out: Collector[String]): Unit = {
    //指的是定时器发生的时间
      out.collect("我是处理时间10s后触发的信息")
    }
  }

}
