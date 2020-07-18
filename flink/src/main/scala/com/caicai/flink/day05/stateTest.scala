package com.atguiug.flink.day05

import java.util

import com.atguiug.flink.day01_03.SensorReading
import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object stateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //启用检查点，设置检查点间隔时间,就是barrier
    env.enableCheckpointing(1000L)
    //其它配置
    //默认 at_least_once
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    //做一次checkpoint的超时时间，如果超过就废弃
    env.getCheckpointConfig.setCheckpointInterval(3000L)
    //做多同时可以做的checkpoint个数
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    //两个相邻checkpoint最短的时间间隔，就是留下的最少的时间来处理第二个checkpoint数据的处理时间
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    //是使用checkpoint来恢复还是用SavePoint来恢复
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    //每一个checkpoint允许失败的次数 默认0
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)

    //重启策略的配置 一些利用checkpoint重启的一些配置策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000L))
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.minutes(1),Time.seconds(1)))
    val inputStream = env.readTextFile("C:\\Scalaa\\flink\\src\\main\\resources\\source.txt")
    inputStream.print()
    val warningStream = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading( dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      } )
        .keyBy("id")
       // .map(new TempChangeWarning(30))
        //.flatMap(new TempChangeWarningFlatmap(30))
//      fun: (T, Option[S]) => (TraversableOnce[R], Option[S])
      /*
      * 输出类型，中间状态 => 输出类型，中间状态
      * 根据Option的值进行模式匹配
      * 就是中间状态就是你要存什么值 这里面需要根据需求，因为这次需要比较每相邻两个值得tempreture
      * */
        .flatMapWithState[(String,Double,Double),Double]({
      case (inputData:SensorReading,None) => (List.empty,Some(inputData.tempreture))
      case (inputDate:SensorReading,lastTemp:Some[Double]) =>

        val diff:Double = (inputDate.tempreture - lastTemp.get).abs
        if(diff > 10.0){
          (List((inputDate.id,lastTemp.get,inputDate.tempreture)),Some(inputDate.tempreture))

        }else{
          (List.empty,Some(inputDate.tempreture))
        }


    })




    env.execute("state test job")

  }
}
//一个输出结果
class TempChangeWarning(thershold:Double) extends RichMapFunction[SensorReading,(String,Double,Double)]{
private var lastTempState:ValueState[Double] = _
  override def open(parameters: Configuration): Unit = {
    //每次一运行，就获取这个值
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lasttempState",classOf[Double]))
  }

  override def map(value: SensorReading): (String, Double, Double) ={
    val lastTemp = lastTempState.value()
    lastTempState.update(value.tempreture)
    val diff = (value.tempreture - lastTemp).abs
    if(thershold < diff){
      (value.id,lastTemp,value.tempreture)
    }else
      (value.id,0,0)
  }
}
//输出多个结果
class TempChangeWarningFlatmap(thershold2:Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)]{
  lazy val flatmapState:ValueState[Double]= getRuntimeContext.getState(new ValueStateDescriptor[Double]("flatMap",classOf[Double]))
  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
   val flatmapvalue = flatmapState.value()
    flatmapState.update(value.tempreture)
    val diff = (flatmapvalue - value.tempreture).abs

    if(diff >thershold2){
      //加括号是告诉System这是一个元祖，而不是collect的三个参数，防止运行时候会报错
      out.collect((value.id,flatmapvalue,value.tempreture))
    }

  }
}



/*
class Myprocessor extends KeyedProcessFunction[String,SensorReading,Int] {
  lazy val mystate = getRuntimeContext.getState(new ValueStateDescriptor[Int]("myInt",classOf[Int]))
  lazy val myListstate = getRuntimeContext.getListState(new ListStateDescriptor[Int]("myListstate",classOf[Int]))
  lazy val myMapstate = getRuntimeContext.getMapState(new MapStateDescriptor[String,Int]("myMapstate",classOf[String],classOf[Int])
  lazy val myReducestate: ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("myreducing",new ReduceFunction[SensorReading] {
   override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
     SensorReading(value1.id,value1.timestamp.max(value2.timestamp),value1.tempreture.min(value2.tempreture))
 },classOf[SensorReading]))
  //另外一种获得state的方式 因为当运行时使用这个mystate才会生效
/*    var mystate:ValueState[Int] = _
  override def open(parameters: Configuration): Unit = {
    val mystate = getRuntimeContext.getState(new ValueStateDescriptor[Int]("myInt",classOf[Int]))

  }*/

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, Int]#Context, out: Collector[Int]): Unit = {
  mystate.value()
    mystate.update(1)

    myListstate.add(3)

   // myListstate.addAll( new util.ArrayList[String]("1","2"))
    myMapstate.put("xiaoming",12)
    myMapstate.putAll(new util.HashMap[String,Int]())

    myReducestate.add(SensorReading("10",111111111,23.0))
  }
}*/
