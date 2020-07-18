package com.caicai.networkflow_analysis
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

//定义输入输出样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )

case class PvCount(windowEnd:Long,count:Long)
object PageView {
  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = inputStream
      .map( (data: String) => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      } )
      .assignAscendingTimestamps((_: UserBehavior).timestamp * 1000L)

    //分配key，包装成而元祖开窗聚合

    val pvStream: DataStream[PvCount] = dataStream
      .filter((_: UserBehavior).behavior == "pv")
    //  .map(data => ("pv",1L))  存在数据倾斜，需要解决这个问题
      .map(new MyMapper())
      .keyBy((_: (String, Long))._1)
      .timeWindow(Time.hours(1))
      .aggregate(new PvCountAgg,new PvCountResult)

    //把各分区的结果汇总起来
    val pvTotalStream = pvStream
        .keyBy((_: PvCount).windowEnd)
       // .process(new pvTotalResult)
      //由于上一个job的分区策略和这个不一样，sum的机制是流式增加，当上游有一个分区a
    //数据来到此次重分区后的subtask（分区）中，会得到一个结果A，当又来一个上游分区b数据就会在A的基础上增加到B,
    //当然A的结果并没有删除，为了除掉多余的A就使用了process了，设置定时任务(和窗口时间一样)，是一个全量增加
    //但是这个就是有两个问题，①数据一直来到，但是中间可能数据太多，会导致内存不够用，OOM，所以使用了布隆过滤器
    //将数据放在redis②，为了除掉没有用的A,就重新定义trigger
        .sum("count")


    println(pvStream)
    env.execute("pv job")
  }

}

class PvCountAgg() extends  AggregateFunction[(String,Long),Long,Long] {
    override def createAccumulator(): Long = 0L
    override def add(value: (_root_.scala.Predef.String, Long),
                     accumulator: Long): Long = accumulator + 1
    override def getResult(accumulator: Long): Long = accumulator
    override def merge(a: Long, b: Long): Long = a+b
  }

class PvCountResult extends WindowFunction[Long,PvCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd,input.head))
  }
}

class MyMapper() extends RichMapFunction[UserBehavior,(String,Long)] {
  val index: Int = getRuntimeContext.getIndexOfThisSubtask
    override def map(value:UserBehavior): (String, Long) = {
      (index.toString,1L)
    }
  }

class pvTotalResult extends KeyedProcessFunction[Long,PvCount,PvCount] {
  lazy val totalCountState: ValueState[Long] =getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-count",classOf[Long]))

  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit ={

    totalCountState.update(value.count + totalCountState.value())

    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]
  ): Unit = {
    out.collect(PvCount(ctx.getCurrentKey,totalCountState.value()))
    totalCountState.clear()
  }
}