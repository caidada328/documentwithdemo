package day04

import day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.filesystem.{AbstractFileStateBackend, FsStateBackend}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.tools.nsc.io.Path


object TempIncreaseAlert {
  def main(args: Array[String]): Unit = {

    val env =StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    //状态后端的保存到hdfs文件
//    val hdfsBackendDir = "hdfs:///flink/checkpoint"
//    env.setStateBackend(new FsStateBackend(hdfsBackendDir))

    val stream = env.addSource(new SensorSource)
      .keyBy(_.id)
      .process(new TempIncreaseAlertFunction)

    stream.print()
    env.execute("TempIncreaseAlert job")
  }
  class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {
    // 用来存储最近一次的温度
    // 当保存检查点的时候，会将状态变量保存到状态后端
    // 默认状态后端是内存，也可以配置hdfs等为状态后端
    // 懒加载，当运行到process方法的时候，才会惰性赋值
    // 状态变量只会被初始化一次
    // 根据`last-temp`这个名字到状态后端去查找，如果状态后端中没有，那么初始化
    // 如果在状态后端中存在`last-temp`的状态变量，直接懒加载
    // 默认值是`0.0`
    lazy val lastTemp = getRuntimeContext.getState(
      new ValueStateDescriptor[Double](
        "last-temp",
        Types.of[Double]
      )
    )

//    lazy val midAndUid = getRuntimeContext.getState(new ListStateDescriptor[String,Set[String]]("hahahha"),Types.of[String,Set[String]]])

    // 存储定时器时间戳的状态变量
    // 默认值是`0L`
    lazy val currentTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long](
        "timer",
        Types.of[Long]
      )
    )

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]
      #Context, out: Collector[String]): Unit = {
      val lastTempreture = lastTemp.value()
      lastTemp.update(value.temperature)
      var currentTimerValue =currentTimer.value()

      if(lastTemp == 0.0 || value.temperature < lastTempreture){

        ctx.timerService().deleteProcessingTimeTimer(currentTimerValue)
        currentTimer.clear()


      }else if( value.temperature > lastTempreture && lastTempreture == 0 ){
        val currentTimerValue = ctx.timerService().currentProcessingTime() + 1 *1000L
        ctx.timerService().registerProcessingTimeTimer(currentTimerValue)
        lastTemp.update(currentTimerValue)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      //#是类型投影，是内部类，要不然无法访问的到

      out.collect("传感器ID为 " + ctx.getCurrentKey + " 的传感器，温度连续1秒钟上升了！")
      currentTimer.clear() // 清空状态变量
    }
  }
}
