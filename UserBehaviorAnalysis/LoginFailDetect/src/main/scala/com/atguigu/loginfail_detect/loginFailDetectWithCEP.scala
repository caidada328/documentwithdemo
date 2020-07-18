package com.caicai.loginfail_detect
import java.util

import com.caicai.loginfail_detect.LoginFail.getClass
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/*改进：不需要在2秒后定时触发而是只要数量满足要求就触发完成输出
*
* */

object loginFailDetectWithCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从文件读取数据，map成样例类，并分配时间戳和watermark
    val resource = getClass.getResource("C:\\Scalaa\\UserBehaviorAnalysis\\MarketAnalysis\\src\\main\\resources\\AdClickLog.csv")
    val loginEventStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
      .map( (data: String) => {
        val dataArray = data.split(",")
        LoginEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })
    //1.定义匹配模式 严格
   /* val loginFailPattern:Pattern[LoginEvent,LoginEvent] = Pattern
      .begin[LoginEvent]("first_fail").where(_.eventTime == "fail") //过滤起一次失败
        .next("second_fail").where(_.eventTime == "fail")
        .within(Time.seconds(2))*/

    val loginFailPattern:Pattern[LoginEvent,LoginEvent] = Pattern
    //过滤起一次失败 非严格
      .begin[LoginEvent]("first_fail").where((_: LoginEvent).eventType == "fail").times(3)
      .within(Time.seconds(4))
    //2.在分组之后的流数据应用模式，得到一个PatternStream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId),loginFailPattern)

    //3将监测到的事件进行输出对应的报警信息
    val loginFailStream = patternStream.select(new LoginFailDetect())
    env.execute("login fail detect   with CEP")
  }

}

//自定义PatternSelectFunction,用来检测到的连续登陆失败时间，包装成报警信息输出

//这个就是有问题就输出
class LoginFailDetect() extends PatternSelectFunction[LoginEvent,Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    //map里存放的就是匹配到的一组事件，key就是定义好的时间模式名称
/* 严格警铃
 val firstLoginFail = map.get("first_fail").get(0)
    val secondLoginFail = map.get("second_fail").get(0)
    Warning(firstLoginFail.userId,firstLoginFail.eventTime,secondLoginFail.eventTime,"login fail 2 times")*/
    //非严格
    val firstLoginFail = map.get("first_fail").get(0)
    val lastLoginFail = map.get("first_fail").get(2)
    Warning(firstLoginFail.userId,firstLoginFail.eventTime,lastLoginFail.eventTime,"login fail 2 times")

  }
}