package day04

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


object EventTimeOnTimer {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("hadoop104",9988,'\n')
      .map(line =>{
        val array =line.split("")
        (array(0),array(1).toLong * 1000L)

      }).assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .process(new MyKeyedProcess)
    stream.print()
    env.execute("KeyedProcessFunctoin")

  }
  class MyKeyedProcess extends KeyedProcessFunction[String,(String,Long),String] {
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {

      ctx.timerService().registerEventTimeTimer(value._2 + 1000 * 10L)
      out.collect("当前水位线是：" + ctx.timerService().currentProcessingTime())
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      out.collect("位于时间戳" + timestamp + "的定时器触发了" )
    }
  }

}
