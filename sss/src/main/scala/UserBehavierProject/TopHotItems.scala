package UserBehavierProject

import java.sql.Timestamp

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

object TopHotItems {
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
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("C:\\Scalaa\\sss\\src\\main\\resources\\UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toLong, arr(3), arr(4).toLong * 1000L)
      })
      .filter(_.behavior.==("pv"))
      .assignAscendingTimestamps(_.timestamp) // 分配升序时间戳
      .keyBy(_.itemId) // 使用商品ID分流
      .timeWindow(Time.hours(1), Time.minutes(5)) // 开窗
      .aggregate(new CountAgg, new WindowResult) // 增量聚合和全窗口聚合结合使用
      .keyBy(_.windowEnd)
      .process(new TopN(3))

    stream.print()
    env.execute()
  }

  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class WindowResult extends ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key, context.window.getEnd, elements.head))
    }
  }

  class TopN(val topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

    lazy val listState: ListState[ItemViewCount] = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("list-state", Types.of[ItemViewCount])
    )

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
      listState.add(value)
      // 不会重复注册
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      // 将列表状态中的数据转移到内存
      for (item <- listState.get) {
        allItems += item
      }
      // 清空状态
      listState.clear()

      // 使用浏览量降序排列
      val sortedItems = allItems.sortBy(-_.count).take(topSize)

      val result = new StringBuilder

      result
        .append("=====================================\n")
        .append("时间：")
        .append(new Timestamp(timestamp - 100))
        .append("\n")

      for (i <- sortedItems.indices) {
        val currItem = sortedItems(i)
        result
          .append("No.")
          .append(i+1)
          .append(":")
          .append("  商品ID = ")
          .append(currItem.itemId)
          .append("  浏览量 = ")
          .append(currItem.count)
          .append("\n")
      }
      result
        .append("=====================================\n\n\n")
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }
}