package com.caicai.hotitems_analysis
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class UserBehavior1(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)

case class ItemViewCount2(itemId:Long,windowEnd:Long,count:Long)
object hotitems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("C:\\Scalaa\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val dataStream = inputStream
      .map( data => {
        val dataArray =data.split(",")
        UserBehavior(dataArray(0).toLong,dataArray(1).toLong,dataArray(2).toInt,dataArray(3),dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val aggStream = dataStream
        .filter(_.behavior == "pv")
        .keyBy("itemId")
        .timeWindow(Time.hours(1),Time.minutes(5))
        .aggregate( new CountAgg1(),new ItemCountWindowResult1())
        .keyBy("windowEnd")
        .process(new TopNHotItems(5))
        .print()

      env.execute(" hot items job")
  }

}


class CountAgg1() extends AggregateFunction[UserBehavior,Long,Long] {
    override def createAccumulator(): Long = 0L
    override def add(value: _root_.com.caicai.hotitems_analysis.UserBehavior,
                     accumulator: Long): Long = accumulator + 1
    override def getResult(accumulator: Long): Long = accumulator
    override def merge(a: Long, b: Long): Long = a + b
  }

class ItemCountWindowResult1() extends WindowFunction[Long,ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window:TimeWindow, input: scala.Iterable[Long], out: Collector[ItemViewCount]
    ): Unit ={
      val itemId = key.asInstanceOf[Tuple1[Long]].f0
      val windowEnd = window.getEnd
      val count = input.iterator.next()
      out.collect(ItemViewCount(itemId,windowEnd,count))
    }
  }

class TopNHotItems1(i: Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String] {

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
    out: Collector[String]
  ): Unit = {

  }}