package com.atguiug.flink.day04

import com.atguiug.flink.day01_03.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream: DataStream[String] = env.socketTextStream("hadoop104",7777)
    val dataStream: DataStream[SensorReading] = inputStream.map(data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })
    //连续10秒温度上升
    val warningStream  =dataStream
      .keyBy("id")
      .process( new TempIncreaseWarning(10000L))
     println(warningStream)

    env.execute("processFunctionTest")

  }

}
//为什么key的类型是tuple，是因为 .keyBy("id")字符串类型，输入类型SensorReading,输出是String类型
class TempIncreaseWarning(interval:Long) extends KeyedProcessFunction[Tuple,SensorReading,String] {
  //关于Collector  Collects a record and forwards it. The collector is the "push" counterpart of the
  // * {@link java.util.Iterator}, which "pulls" data in.
  //由于需要和之前的温度值做对比，所以需要将上一个温度状态进行保存 --->使用状态编程
  lazy val lastTimeState:ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTimeState",classOf[Double]))
  //还需要根据需求删除不符合要求数据的定时器，所以需要保存一系列定时器的时间戳

  lazy val curTimeTsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("curTimeState",classOf[Long]))


  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
   //首先取出状态  这个如果初始值没有的话，就是默认为0的
    val lastTime = lastTimeState.value()
    val curTimeTs = curTimeTsState.value()

    //将上次温度的状态更新为当前的温度值
    lastTimeState.update(value.tempreture)

    //如果温度处于上升趋势，更新状态 curTimeTs == 0 是因为一般这个值如果你没有注册，默认值是0
    if(value.tempreture > lastTime && curTimeTs == 0){
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      curTimeTsState.update(ts)
    }
      //如果状态处于下降趋势，将定时器删除
    else if(value.tempreture < lastTime){
      ctx.timerService().deleteProcessingTimeTimer(curTimeTs)
    }
    curTimeTsState.clear()
  }

  //这个是定时器触发机制，十秒内温度一直攀升，就报警
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {


    out.collect("温度连续" + interval/1000 + "秒上升")
    //每次都要清一下上个状态
    curTimeTsState.clear()
  }

  override def toString: String = super.toString
}
