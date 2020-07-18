package com.caicai.orderpay_detect
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTransactionOnMatchWithIntervalJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 从文件中读取数据，并转换成样例类

    val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile("C:\\Scalaa\\UserBehaviorAnalysis\\orderPayDetect\\src\\main\\resources\\OrderLog.csv")
      //      val orderEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
      val dataArray = data.split(",")
      OrderEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
    } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })
      .filter(_.txId != "")     // 只过滤出pay事件
      .keyBy(_.txId)


    val receiptEventStream: KeyedStream[ReceiptEvent, String] = env.readTextFile("C:\\Scalaa\\UserBehaviorAnalysis\\orderPayDetect\\src\\main\\resources\\ReceiptLog.csv")
      //      val orderEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
      val dataArray = data.split(",")
      ReceiptEvent( dataArray(0), dataArray(1), dataArray(2).toLong )
    } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
        override def extractTimestamp(element: ReceiptEvent): Long = element.timestamp * 1000L
      })
      .keyBy(_.txId)
  //    .mapWithState()

    // 使用join连接两条流
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream
      .intervalJoin(receiptEventStream)
      .between( Time.seconds(-3), Time.seconds(5) )
      .process( new OrderPayTxDetectWithJoin() )

    resultStream.print()
    env.execute("order pay tx match with join job")
  }
}

// 自定义ProcessJoinFunction
class OrderPayTxDetectWithJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect( (left, right) )
  }
}
