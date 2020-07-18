package com.caicai.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
//定义输入数据样例类
case class ApacheLogEvent (ip:String,userId:String,eventTime:Long,method:String,url:String)
//定义聚合结果样例类
case class PageViewCount(url:String,windowEnd:Long,count:Long)
object networkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("C:\\Scalaa\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")

    //转换成样例类类型，指定timestamp和watermark
    val dataStream = inputStream
      .map(data =>{
        val dataArray = data.split( "")
        //将网页的时间格式转换为时间戳
        val f = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = f.parse(dataArray(3)).getTime
        ApacheLogEvent(dataArray(0),dataArray(1),timestamp,dataArray(5),dataArray(6))
      })
    //处理乱序数据 ，使用这个获得watermark和eventTime
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.minutes(5)) {
        override def extractTimestamp(
      element: ApacheLogEvent
    ): Long = element.eventTime
      })
    //开窗聚合

    val aggregateStream = dataStream
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.seconds(5))
    //在二者之间设置迟到数据处理允许时间
      .allowedLateness(Time.minutes(1))
    //最终延迟数据处理逻辑
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late data"))
      .aggregate(new pageCountAgg(),new pageCountWindowResult())

    //处理最终延迟的数据，这个数据类似批处理离线操作了，后续如何操作，就从这里面获得数据
    val lateDataStream = aggregateStream.getSideOutput(new OutputTag[ApacheLogEvent]("late data"))

    //每个窗口的统计值排序输出

    val resultStream = aggregateStream
      .keyBy(_.windowEnd)
      .process(new TOpNHotPage(3))

    resultStream.print()

    env.execute("top npages job ")
  }
}

class pageCountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long] {
    override def createAccumulator(): Long = 0L
    override def add(value:ApacheLogEvent, accumulator: Long): Long = accumulator + 1
    override def getResult(accumulator: Long): Long = accumulator
    override def merge(a: Long, b: Long): Long = a +b
  }

//自定义windowFunction,包装成样例类输出

class pageCountWindowResult() extends WindowFunction[Long,PageViewCount, String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
   out.collect(PageViewCount(key,window.getEnd,input.head))
  }
}

//自定义processFunction

class TOpNHotPage(i: Int) extends KeyedProcessFunction[Long,PageViewCount,String] {
  lazy val  pageCountMapState:MapState[String,Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String,Long]("pagecount-list",classOf[String],classOf[Long]))
    override def processElement(value: PageViewCount, ctx:KeyedProcessFunction[Long, PageViewCount, String]#Context, out:Collector[String]
    ): Unit ={
      pageCountMapState.put(value.url,value.count)

      //何时触发定时任务
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
      //用来清空状态，一分钟延迟考虑上去
      ctx.timerService().registerEventTimeTimer(value.windowEnd+60*1000L)
    }
  //定时任务的具体实现
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]
  ): Unit = {
    if(timestamp == ctx.getCurrentKey + 60*1000L){
      pageCountMapState.clear()
      return
    }
    val allPageCountList:ListBuffer[(String,Long)] = ListBuffer()

    val iter = pageCountMapState.entries().iterator()
    while(iter.hasNext){
      val entry = iter.next()

      allPageCountList += ((entry.getKey,entry.getValue))

    }
    //pageCountListState.clear()

    val sortedPageCountList = allPageCountList.sortWith(_._2 > _._2).take(i)

    val result:StringBuffer = new StringBuffer
    result.append("时间 ;").append(new Timestamp(timestamp - 1)).append("\n")

    for(i <- sortedPageCountList.indices){
      val currentItemCount = sortedPageCountList(i)

      result.append("Top").append(i+1).append(":")
        .append("页面url=").append(currentItemCount._1)
        .append("访问量=").append(currentItemCount._2)
        .append("\n")
    }
    result.append("====================\n\n")
  }}