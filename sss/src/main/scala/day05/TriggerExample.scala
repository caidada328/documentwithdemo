package day05

import java.sql.Timestamp

import day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object TriggerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .trigger(new OneSecondIntervaTrigger)
      .process(new WindowResult)

    stream.print()

    env.execute("trigger example")
  }



  class WindowResult extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {

    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
      out.collect("传感器的Id" + key + "传感器数量是" + elements.size)
    }
  }

  class OneSecondIntervaTrigger extends Trigger[SensorReading, TimeWindow] {

    //类似keyedProcessFucntion
    override def onElement(element: SensorReading, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      val firstSeen: ValueState[Boolean] = ctx.getPartitionedState(new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean]))

      if (firstSeen.value()) {
        val t = ctx.getCurrentProcessingTime + (1000 - (ctx.getCurrentProcessingTime % 1000))
        ctx.registerProcessingTimeTimer(t)
        ctx.registerProcessingTimeTimer(window.getEnd)

        firstSeen.update(true)
      }
      TriggerResult.CONTINUE

    }

     //由于没有设置时间策略，默认是ProcessingTime所以说就是使用这个做为触发机制
    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      //这个时间有两种，当对应的t 或者 window.getEnd,那么就是打印对应的时间
       print("回调函数的触发时间" + new Timestamp(time))
      if(time == window.getEnd){

           TriggerResult.FIRE_AND_PURGE
      }else{
             val t = ctx.getCurrentProcessingTime + (1000 -(ctx.getCurrentProcessingTime%1000))
        if(t < window.getEnd){
          ctx.deleteProcessingTimeTimer(t)
        }
        TriggerResult.FIRE
      }
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      //因为triggrt内部不能实现全局变量，为了从状态后端获得这个值必须重新设置
      val firstSeen: ValueState[Boolean] = ctx.getPartitionedState(new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean]))
      firstSeen.clear()

    }
  }

}