package com.caicai.networkflow_analysis
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class UvCount(windowEnd:Long,count:Long)
object UniqueVisit {
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


    //全窗口函数的操作 全量聚合
  /*  val UvStream: DataStream[UvCount] = dataStream
      .filter((_: UserBehavior).behavior == "pv")
      .timeWindowAll(Time.hours(1))//一个小时完后才把数据给输出，不符合流式数据处理
      .apply(new UvCountResult())*/

    //增量聚合
      val UvStream: DataStream[UvCount] = dataStream
      .filter((_: UserBehavior).behavior == "pv")
      .timeWindowAll(Time.hours(1))
        .aggregate(new UvCountAgg(), new UvCountResultWithIncreAgg())


    println(UvStream)

    env.execute(" Uv job")
  }

}

class UvCountResult() extends AllWindowFunction[UserBehavior,UvCount,TimeWindow] {
    override def apply(window:TimeWindow, input: Iterable[UserBehavior], out: _root_.org.apache.flink.util.Collector
      [_root_.com.caicai.networkflow_analysis.UvCount]): Unit = {
      var idSet = Set[Long]()
      //利用set将UserId存储起来 但是这个可能用Set来储存，可能会造成OOM,1：使用trigger,布隆过滤器，2：或者这个保存在Redis，但比较困难
      //自定义trigger在全窗口函数可以设置触发机制，这个主要处理大数据的内存

      for(userBehavior <- input){
        idSet += userBehavior.userId
      }
      //将去重后的数据输出
      out.collect(UvCount(window.getEnd,idSet.size))
    }
  }

class UvCountAgg() extends AggregateFunction[UserBehavior,Set[Long],Long] {
    override def createAccumulator(): _root_.scala.Predef.Set[Long] = Set[Long]()
    override def add(value: UserBehavior, accumulator: Set[Long]):
    Set[Long] = accumulator + value.userId
    override def getResult(accumulator: Set[Long]): Long = accumulator.size
    override def merge(a: Set[Long], b:Set[Long]): Set[Long] = a++b
  }

class UvCountResultWithIncreAgg() extends AllWindowFunction[Long,UvCount,TimeWindow] {
    override def apply(window: TimeWindow, input:Iterable[Long], out: Collector[UvCount]): Unit = {
      out.collect(UvCount(window.getEnd,input.head))
    }
  }