package com.caicai.orderpay_detect
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

case class OrderEvent1(orderId:Long,eventType:String,txId:String,eventTime:Long)
case class ReceiptEvent( txId: String, payChannel: String, timestamp: Long )
object OrderTrsnsactionMatch {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderEventStream: DataStream[OrderEvent1] = env.readTextFile("C:\\Scalaa\\UserBehaviorAnalysis\\orderPayDetect\\src\\main\\resources\\OrderLog.csv")
      //      val orderEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
      val dataArray = data.split(",")
      OrderEvent1( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
    } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent1](Time.seconds(3)) {
        override def extractTimestamp(element: OrderEvent1): Long = element.eventTime * 1000L
      })
      .filter(_.txId != "")     // 只过滤出pay事件
      .keyBy(_.txId)

    //val resource2 = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream: DataStream[ReceiptEvent] = env.readTextFile("C:\\Scalaa\\UserBehaviorAnalysis\\orderPayDetect\\src\\main\\resources\\ReceiptLog.csv")
      //      val orderEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
      val dataArray = data.split(",")
      ReceiptEvent( dataArray(0), dataArray(1), dataArray(2).toLong )
    } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
        override def extractTimestamp(element: ReceiptEvent): Long = element.timestamp * 1000L
      })
      .keyBy(_.txId)

    // 用connect连接两条流，匹配事件进行处理
    val resultStream: DataStream[(OrderEvent1, ReceiptEvent)] =  orderEventStream
      .connect(receiptEventStream)
      .process(new OrderPayTxDelete)

    val unmatchedPays = new OutputTag[OrderEvent1]("unmatched-pays")
    val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatched-receipts")

    resultStream.print("matched")
    resultStream.getSideOutput(unmatchedPays).print("unmatched-pays")
    resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts")
    env.execute("order pay tx match job")

  }

  class OrderPayTxDelete extends CoProcessFunction[OrderEvent1,ReceiptEvent,(OrderEvent1,ReceiptEvent)] {

    lazy val payState: ValueState[OrderEvent1] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent1]("pay", classOf[OrderEvent1]))
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))

    val unmatchedPays = new OutputTag[OrderEvent1]("unmatched-pays")
    val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatched-receipts")

    override def processElement1(pay: OrderEvent1, ctx: CoProcessFunction[OrderEvent1, ReceiptEvent, (OrderEvent1, ReceiptEvent)]#Context,
                                 out:Collector[(OrderEvent1, ReceiptEvent)]): Unit = {
      //pay来了，但是对应的receipt还没来
      val receipt = receiptState.value()
      if(receipt != null){
        //如果有reciept，那么就正常匹配，输出到主流  ??????正常输出到主流的payState状态为什么不删除
        out.collect((pay,receipt))
        receiptState.clear()
      }else{
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer( pay.eventTime * 1000L + 5000L )
      }


    }
    override def processElement2(receipt:ReceiptEvent, ctx:CoProcessFunction[OrderEvent1, ReceiptEvent, (OrderEvent1, ReceiptEvent)]#Context,
                                 out: Collector[(OrderEvent1, ReceiptEvent)]): Unit = {
      // receipt来了，考察有没有对应的pay来过
      val pay = payState.value()
      if( pay != null ){
        // 如果已经有pay，那么正常匹配，输出到主流
        out.collect( (pay, receipt) )
        payState.clear()
      } else{
        // 如果pay还没来，那么把receipt存入状态，注册一个定时器等待3秒
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer( receipt.timestamp * 1000L + 3000L )
      }
    }

    // 定时器触发，有两种情况，所以要判断当前有没有pay和receipt  //可以返回空值的，相当于全外连接
    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent1, ReceiptEvent, (OrderEvent1, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent1, ReceiptEvent)]): Unit = {
      // 如果pay不为空，说明receipt没来，输出unmatchedPays
      if( payState.value() != null )
        ctx.output(unmatchedPays, payState.value())
      if( receiptState.value() != null )
        ctx.output(unmatchedReceipts, receiptState.value())
      // 清空状态
      payState.clear()
      receiptState.clear()
    }
  }

}


