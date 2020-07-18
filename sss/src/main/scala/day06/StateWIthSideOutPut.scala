package day06

import day2. SensorSource
import org.apache.flink.api.common.functions.{RichFlatJoinFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object StateWIthSideOutPut {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val resultSource = env.addSource( new SensorSource)
      .keyBy(_.id)
      .map(sen => sen.temperature)
      .flatMap(new MyFlatMapWithState(1.7))
      .print()

    env.execute("flatmapwithstate job")
  }
  class MyFlatMapWithState(val diff:Double) extends RichFlatMapFunction[Double,String] {
       var lastTemp:ValueState[Double] = _
    override def open(parameters: Configuration): Unit = {
      lastTemp = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastvalue",classOf[Double]))
    }

    override def flatMap(value: Double, out: Collector[String]): Unit = {
     val  lastvalue = lastTemp.value()
      lastTemp.update(value)
      if(value-lastvalue > diff){
        out.collect(s"${value}:超温极限了")
      }
    }
  }
}
