package com.caicai.market_analysis
import java.sql.Timestamp


import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object AppMarketingTotal {
  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      env.setParallelism(1)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      val dataStream = env.addSource(new SimulateMarketEventSource())
        .assignAscendingTimestamps(_.timeStamp)

      val resultStream = dataStream
        .filter(_.behavior != "UNINSTALL")
        .map(data => ("total",1L))
        .keyBy(_._1)
        .timeWindow(Time.hours(1),Time.seconds(5))
        //自定义全窗口函数
        .aggregate(new MarketCountAgg(),new MarketCountTotalResult())

      resultStream.print()

      env.execute("market total count")


    }
  }

}

class MarketCountAgg() extends AggregateFunction[(String,Long),Long,Long] {
    override def createAccumulator(): Long = 0L
    override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1
    override def getResult(accumulator: Long): Long = accumulator
    override def merge(a: Long, b: Long): Long = a+b
  }

class MarketCountTotalResult() extends WindowFunction[Long,MarketCOunt,String, TimeWindow] {
    override def apply(
      key: _root_.scala.Predef.String,
      window: _root_.org.apache.flink.streaming.api.windowing.windows.TimeWindow,
      input: scala.Iterable[Long],
      out: _root_.org.apache.flink.util.Collector[
        _root_.com.caicai.market_analysis.MarketCOunt
      ]
    ): Unit ={
      val windowStart = new Timestamp(window.getStart).toString
      val windowEnd = new Timestamp(window.getEnd).toString
      val count = input.head
      out.collect( MarketCOunt(windowStart,windowEnd,"total","total",count))
    }
  }
