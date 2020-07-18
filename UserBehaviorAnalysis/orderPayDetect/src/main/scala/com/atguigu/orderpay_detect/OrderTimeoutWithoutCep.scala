package com.caicai.orderpay_detect
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector



case class Orderevent(orderId:Long,eventType:String,txId:String,eventTime:Long)

case class OrderResult1(orderId:Long,OrderResultMsg:String)
object OrderTimeoutWithoutCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //val resource = getClass.getResource("C:\\Scalaa\\UserBehaviorAnalysis\\orderPayDetect\\src\\main\\resources\\OrderLog.csv")
    val orderEventStream = env.readTextFile("C:\\Scalaa\\UserBehaviorAnalysis\\orderPayDetect\\src\\main\\resources\\OrderLog.csv")
      .map(data =>{
        val dataArray = data.split(",")
        Orderevent(dataArray(0).toLong,dataArray(1),dataArray(2),dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Orderevent](Time.seconds(3)) {
        override def extractTimestamp(element: Orderevent): Long = element.eventTime * 1000L
      })

    val orderResultStream = orderEventStream
      .keyBy(_.orderId)
      .process(new OrderPayMatchDetect ())


    println(orderResultStream)
    env.execute("ordertime without cep job")
  }

}


class OrderPayMatchDetect() extends KeyedProcessFunction[Long, Orderevent, OrderResult1] {
  // 定义状态，用来保存是否来过create和pay事件的标识位，以及定时器时间戳
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed", classOf[Boolean]))
  lazy val isCreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  val orderTimeoutOutputTag = new OutputTag[OrderResult1]("timeout")

  override def processElement(value: Orderevent, ctx: KeyedProcessFunction[Long, Orderevent, OrderResult1]#Context, out: Collector[OrderResult1]): Unit = {
    // 先取出当前状态
    val isPayed = isPayedState.value()
    val isCreated = isCreatedState.value()
    val timerTs = timerTsState.value()

    // 判断当前事件的类型，分成不同情况讨论
    // 情况1： 来的是create，要继续判断之前是否有pay来过
    if (value.eventType == "create") {
      // 情况1.1： 如果已经pay过的话，匹配成功，输出到主流，清空状态
      // 问题1.如果先pay后create 乱序为什么不考虑时间超过15minutes 呢
      //问题2：为啥不在67,92删除另外两种状态呢
      if (isPayed) {
        out.collect(OrderResult1(value.orderId, "payed successfully"))
        isPayedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      }
      // 情况1.2：如果没pay过，那么就注册一个15分钟后的定时器，更新状态，开始等待
      else {
        val ts = value.eventTime * 1000L + 15 * 60 * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
        isCreatedState.update(true)
      }
    }
    // 情况2： 来的是pay，要继续判断是否来过create
    else if (value.eventType == "pay") {
      // 情况2.1：如果create已经来过，匹配成功，要继续判断间隔时间是否超过了15分钟
      if (isCreated) {
        // 情况2.1.1： 如果没有超时，正常输出结果到主流
        if (value.eventTime * 1000L < timerTs) {
          out.collect(OrderResult1(value.orderId, "payed successfully"))
        }
        // 情况2.1.2： 如果已经超时，输出timeout报警到侧输出流
        else {
          ctx.output(orderTimeoutOutputTag, OrderResult1(value.orderId, "payed but already timeout"))
        }
        // 不论哪种情况，都已经有了输出，清空状态
        isCreatedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      }
      // 情况2.2：如果create没来，需要等待乱序create，注册一个当前pay时间戳的定时器
      else {
        val ts = value.eventTime * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
        isPayedState.update(true)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, Orderevent, OrderResult1]#OnTimerContext, out: Collector[OrderResult1]): Unit = {
    // 定时器触发，要判断是哪种情况
    if (isPayedState.value()) {
      // 如果pay过，那么说明create没来，可能出现数据丢失异常情况
      ctx.output(orderTimeoutOutputTag, OrderResult1(ctx.getCurrentKey, "already payed but not found created log"))
    } else{
      // 如果没有pay过，那么说明真正15分钟超时
      ctx.output(orderTimeoutOutputTag, OrderResult1(ctx.getCurrentKey, "order timeout"))
    }
    // 清理状态
    isPayedState.clear()
    isCreatedState.clear()
    timerTsState.clear()
  }
}
