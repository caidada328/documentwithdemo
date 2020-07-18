package day04

import day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator
import org.apache.flink.util.Collector


object SideOutputExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource).process( new MyProcessF)
    stream.getSideOutput(new OutputTag[String]("freezing-alarms")).print()
    env.execute("sideOutputExample")

  }
  class MyProcessF extends ProcessFunction[SensorReading,SensorReading] {
    lazy val  freezingAlarmOutput = new OutputTag[String]("freezing-alarms")

    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature < 32.0){
        ctx.output(freezingAlarmOutput,s"${value.id}的温度高于32度")
      }
      out.collect(value)
    }
  }
}
