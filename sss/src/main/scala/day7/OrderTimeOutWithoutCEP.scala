package day7

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeOutWithoutCEP {
  case class OrderEvent(userId:String,EventType:String,EventTime:Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rawStream = env.fromElements(
      OrderEvent("user_1","create",2000L),
      OrderEvent("user_2","create",2500L),
      OrderEvent("user_2","pay",3000L)
    )
      .assignAscendingTimestamps(_.EventTime)
    val timeOutTap = new OutputTag[String]("timeout")
    val resultStream = rawStream.keyBy(_.userId)
      .process(new MyOrderTimeOutFunc)
    resultStream.print()
    resultStream.getSideOutput[String](timeOutTap).print()
    resultStream.print()
    env.execute("ordertimeoutwithoutCEP job")
  }

  class MyOrderTimeOutFunc extends KeyedProcessFunction[String,OrderEvent,String] {

    lazy val eventState = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("valuestate",classOf[OrderEvent]))
    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[String, OrderEvent, String]#Context, out: Collector[String]): Unit = {
      if(value.EventType.equals("create")){
        val timers = value.EventTime
        if(eventState.value == null){
          ctx.timerService().registerEventTimeTimer(timers + 5 * 1000L)
          eventState.update(value)
        }else{
          eventState.update(value)
          out.collect("没有超时的时间的Id:" + value.userId)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, OrderEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
     val nonOvertimeId = eventState.value().userId
      ctx.output[String](new OutputTag[String]("timeout"),"超时的ID是" + nonOvertimeId)
    }
  }

}
