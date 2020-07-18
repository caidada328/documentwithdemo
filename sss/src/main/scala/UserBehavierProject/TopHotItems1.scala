package UserBehavierProject

import java.sql
import java.sql.Timestamp

import UserBehavierProject.TopHotItems.ItemViewCount
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


object TopHotItems1 {

  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Long,
                          behavior: String,
                          timestamp: Long)

  case class ItemViewCount(itemId: Long,
                           windowEnd: Long,
                           count: Long)


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resultStream = env.readTextFile("C:\\Scalaa\\sss\\src\\main\\resources\\UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong,arr(1).toLong,arr(2).toLong,arr(3),arr(4).toLong * 1000L)

      })
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior =="pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1),Time.minutes(5))
      .aggregate(new MyAggFun,new ResultWindowFun)
      .keyBy(_.windowEnd)
        .process(new MyProcessFunc(3))

    env.execute()
  }

  class MyAggFun extends AggregateFunction[UserBehavior,Long,Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator +1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }


  class ResultWindowFun extends ProcessWindowFunction[Long,ItemViewCount,Long,TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key,context.window.getEnd,elements.head))
    }



  }

  class MyProcessFunc(val TopNumber:Int) extends KeyedProcessFunction[Long,ItemViewCount,String] {
    lazy val top3State: ListState[ItemViewCount] = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("list-state", Types.of[ItemViewCount])
    )
    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
      top3State.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1000L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val lbTops3: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for(item <- top3State.get){
        lbTops3 += item
      }
      top3State.clear()
      val sortedTop3Result = lbTops3.sortBy(-_.count) .take(TopNumber)

      val sbResult  = new StringBuilder
      sbResult.append("====================================" + "\n\n")
      sbResult.append("======当前时间窗口是+" + new Timestamp(timestamp - 1000) + "======")
      for(i <- sortedTop3Result.indices){
        sbResult.append("No." + (i + 1) + ":的商品ID是" + sortedTop3Result(i).itemId + "\t\t\t" + "总的PV是"  + sortedTop3Result(i).count)
      }
      sbResult.append("====================================" + "\n\n")
      out.collect(sbResult.toString())
    }
  }
}

