package day05

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


object WindowJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val orangeStream = env.fromElements((1, 1000L), (1, 999L))
      .assignAscendingTimestamps(_._2)

    val greenStream = env.fromElements((1, 1001L), (1, 1002L))
      .assignAscendingTimestamps(_._2)
    // 做的是笛卡尔积
    orangeStream.join(greenStream)
      .where(r => r._1)
      .equalTo(r => r._1)
      //join了之后再windw，保证数据之间窗口严格对齐
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply { (e1, e2) => e1 + "*****" + e2 }
      .print()
    /*
    * (1,1000)*****(1,1001)
    ( 1,1000)*****(1,1002)
    (1,999)*****(1,1001)
     (1,999)*****(1,1002)
    *
    * */

    env.execute("双流join")
  }
}
