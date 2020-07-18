package com.caicai.orderpay_detect
import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/*
* 实时监控订单支付状态，超过15分钟，做出对应的处理
* */

case class OrderEvent(orderId:Long,eventType:String,txId:String,eventTime:Long)

case class OrderResult(orderId:Long,OrderResultMsg:String)
object OrderPayDetect {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //val resource = getClass.getResource("C:\\Scalaa\\UserBehaviorAnalysis\\orderPayDetect\\src\\main\\resources\\OrderLog.csv")
    val dataStream = env.readTextFile("C:\\Scalaa\\UserBehaviorAnalysis\\orderPayDetect\\src\\main\\resources\\OrderLog.csv")
      .map(data =>{
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong,dataArray(1),dataArray(2),dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })

    val orderPayPattern = Pattern.begin[OrderEvent]("create").where(_.eventType == "create")
      //事件触发是在同一个订单号的最后一个事件对应事件触发才触发，因为比如当create和pay全部出现，
      // pay的时间是10s(waterMark是3Seconds)那么当13s(当然这个时候waterMark是10s)的时间来了才行,在不开窗口就是来一个结合waterMark来输出
     //如果说乱序中有数据大于理论延迟时间，如果不处理，就是放在测输出流中，就会丢失
      .followedBy("pay").where(_.eventType == "pay")

      .within(Time.minutes(15))

    val patternStream  = CEP.pattern(dataStream.keyBy(_.orderId),orderPayPattern)


    //3.用户15分钟后才支付订单，这个就是超时事件，所以需要在侧输出流来获得数据
    val orderTimeOutOutPutTag = new OutputTag[OrderResult]("pay timeout")

    //4.调用select方法，提取匹配事件和超时时间，分别进行处理转换输出
    val resultStream = patternStream
      .select(orderTimeOutOutPutTag,new OrderTimeoutSelect(),new OrderPaySelect())

    resultStream.print("payed")
    //获取超时的数据，这个超时是在CEP中的超时事件 当然可以在CEP中定义sideOutput的数据
    resultStream.getSideOutput(orderTimeOutOutPutTag).print("timeout")

    env.execute("order pay detect")
  }
}
//自定义处理超时数据 pay中没有数据
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent,OrderResult] {
    override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
      val tiemoutOrderId: Long = map.get("create").iterator().next().orderId
      OrderResult(tiemoutOrderId,"timeout at" + l )
    }
  }

//自定义处理正常数据
class OrderPaySelect() extends PatternSelectFunction[OrderEvent,OrderResult] {
    override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
      val payOrderId: Long = map.get("pay").iterator().next().orderId
      OrderResult(payOrderId,"payed successful")
    }
  }