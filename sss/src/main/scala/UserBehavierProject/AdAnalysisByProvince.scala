package UserBehavierProject

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class AdclickEvent(userId:Long,adId:Long,province:String,city:String,timestamp:Long)
case class AdCountByProvince(province:String,windowEnd:String,count:Long)
case class BlackListWarning( userId: Long, adId: Long, msg: String)
object AdAnalysisByGeo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    /*    val source = getClass.getResource("C:\\Scalaa\\UserBehaviorAnalysis\\MarketAnalysis\\src\\main\\resources\\AdClickLog.csv")

        val adLogStream = env.readTextFile(source.getPath)*/
    val adLogStream = env.readTextFile("C:\\Scalaa\\UserBehaviorAnalysis\\MarketAnalysis\\src\\main\\resources\\AdClickLog.csv")
      .map(data =>{
        val dataArray = data.split(",")

        AdclickEvent(dataArray(0).toLong,dataArray(1).toLong,dataArray(2),dataArray(3),dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp*1000L)

    //定义刷单行为过滤操作

    val filterBlackList:DataStream[AdclickEvent] = adLogStream
      .keyBy(data =>(data.userId,data.adId))
      //自定义的KeyedProcessFunction既可以用做中间件来阻断数据，也可以作为输出结果的处理逻辑
      .process(new FilterBlackList(100L))

    val adCountStream = filterBlackList
      .keyBy(_.province)
      .timeWindow(Time.hours(1),Time.seconds(5))
      .aggregate(new AdcountAgg(),new AdcountResult())


    adCountStream.print()
    filterBlackList.getSideOutput(new OutputTag[BlackListWarning]("blacklist")).print("blackList")

    env.execute(" ad analysis geo")

  }

}

class AdcountAgg() extends AggregateFunction[AdclickEvent,Long,Long] {
  override def createAccumulator(): Long = 0L
  override def add(value:AdclickEvent,
                   accumulator: Long): Long = accumulator+1
  override def getResult(accumulator: Long): Long = accumulator
  override def merge(a: Long, b: Long): Long = a + b
}

class AdcountResult() extends WindowFunction[Long,AdCountByProvince,String,TimeWindow] {
  override def apply(
                      key: String,
                      window: TimeWindow,
                      input: Iterable[Long],
                      out: Collector[
                        AdCountByProvince
                        ]
                    ): Unit = {
    val province = key
    val windowEnd = new Timestamp(window.getEnd).toString
    val count = input.iterator.next()

    out.collect(AdCountByProvince(province,windowEnd,count))
  }
}

class FilterBlackList(maxCLickCount: Long) extends KeyedProcessFunction[(Long,Long),AdclickEvent,AdclickEvent]{
  lazy val countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count",classOf[Long]))

  lazy val isSentState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isSent",classOf[Boolean]))

  override def processElement(value:AdclickEvent, ctx: KeyedProcessFunction[(Long, Long), AdclickEvent, AdclickEvent]#Context, out:Collector[AdclickEvent]): Unit = {
    val curCount = countState.value()
    if(curCount == 0){
      //第一次来进行注册，用来控制这对数据在第二天凌晨0:00触发操作
      val ts = (ctx.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * (1000*60*60*24)
      //注意注册的时间是processtime
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    //将不符合要求的，有恶意刷单的给屏蔽掉
    if(curCount >= maxCLickCount ){
      if(!isSentState.value()){
        //原来在这里也可以实现延迟数据批处理的操作
        ctx.output(new OutputTag[BlackListWarning]("blacklist"),BlackListWarning(value.userId,value.adId,"click over" + maxCLickCount + "times today"))
        isSentState.update(true)
      }
      //直接返回，不需要继续对数据进行叠加
      return
    }
    countState.update(curCount + 1)
    out.collect(value)
  }
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdclickEvent, AdclickEvent]#OnTimerContext, out: Collector[AdclickEvent]
                      ): Unit = {
    //每天0点触发定时器，直接清空状态
    countState.clear()
    isSentState.clear()
  }
}