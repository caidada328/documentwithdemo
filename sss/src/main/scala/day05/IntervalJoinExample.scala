package day05

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

//inner join
object IntervalJoinExample {
  case class UserCLickLog(userID:String,
                          eventTime:String,
                          eventType:String,
                          pageID:String)

  case class UserBrowseLog(userID:String,
                           eventTime: String,
                           eventType:String,
                           productID:String,
                           productPrice:String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val clickStream = env.fromElements(
      UserCLickLog("user_1","1500","click","page_1"),
      UserCLickLog("user_2","2000","click","page_1")
    )
      .assignAscendingTimestamps(_.eventTime.toLong*1000L)
      .keyBy(_.userID)

    val browseStream = env .fromElements(
      UserBrowseLog("user_2", "1000", "browse", "product_1","10"),
      UserBrowseLog("user_2", "1500", "browse", "product_1", "10"),
      UserBrowseLog("user_2", "1501", "browse", "product_1", "10"),
      UserBrowseLog("user_2", "1502", "browse", "product_1", "10")
    )
      .assignAscendingTimestamps(_.eventTime.toLong*1000L)
      .keyBy(_.userID)

    clickStream
      //左开右闭区间
      .intervalJoin(browseStream)
      .between(Time.minutes(-10),Time.minutes(0))
      .process(new  MyIntervalJoin)
      .print()
    env.execute("interval join && ProcessJoinFunciton job")
  }
     class MyIntervalJoin extends ProcessJoinFunction[UserCLickLog,UserBrowseLog,String] {
       override def processElement(left: UserCLickLog, right: UserBrowseLog, ctx: ProcessJoinFunction[UserCLickLog, UserBrowseLog, String]#Context,
                                   out: Collector[String]): Unit = {
         out.collect(left + "====" + right)
       }


     }
}
