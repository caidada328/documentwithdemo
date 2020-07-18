package com.caicai.market_analysis
import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

//定义输入数据样例类
case class  MarketUserBehavior(userId:String,behavior:String,channel:String,timeStamp:Long)
//定义输出统计的样例类
case class MarketCOunt(windowStart:String,windowEnd:String,channel:String,behavior:String,count:Long)
//自定义测试数据源
class SimulateMarketEventSource() extends RichParallelSourceFunction[MarketUserBehavior] {

  //定义是否在运行的标志
  var running =  true
  //定义用户行为和推广渠道的集合
  val behaviorSet: Seq[String] = Seq("CLICK","DOWNLOAD","INSTALL","UNINSTALL")
  val channelSet: Seq[String] = Seq("appStore","huaweiStore","weibo","wechat")
  //定义随机生成器
  val rand: Random.type = Random

    override def run(ctx:SourceContext[MarketUserBehavior]): Unit = {
      //定义一个发出数据的最大值
      val maxCounts = Long.MaxValue
      var count = 0L

      //不断随机生成数值
      while(running && count < maxCounts){
        val id = UUID.randomUUID().toString
        val behavior: String = behaviorSet(rand.nextInt(behaviorSet.size))
        val channel = channelSet(rand.nextInt(channelSet.size))

        val ts = System.currentTimeMillis()
        ctx.collect(MarketUserBehavior(id,behavior,channel,ts))

        count += 1
        Thread.sleep(1000)

      }
    }
    override def cancel(): Unit = running = false
  }
class AppMarketByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource(new SimulateMarketEventSource())
      .assignAscendingTimestamps(_.timeStamp)

    val resultStream = dataStream
    .filter(_.behavior != "UNINSTALL")
      .keyBy( (data: MarketUserBehavior) => (data.channel,data.behavior))
      .timeWindow(Time.hours(1),Time.seconds(5))
    //自定义全窗口函数
      .process(new MarketCountByChannel())

    resultStream.print()

    env.execute("market count by  channel")


  }

}
//自定义ProcessWindowFunction相对于一般的apply中定义的 processFunciton和WIndowFunction的结合
class  MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior,MarketCOunt,(String,String),TimeWindow] {
    override def process(key:(String,String) , context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketCOunt]): Unit = {
      val windowStart = new Timestamp(context.window.getStart).toString
      val windowEnd = new Timestamp(context.window.getEnd).toString
      val channel:String = key._1
      val behavior = key._2
      val count = elements.size
      out.collect(MarketCOunt(windowStart,windowEnd,channel,behavior,count))
    }
  }
