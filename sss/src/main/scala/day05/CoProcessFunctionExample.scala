package day05

import day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator
import org.apache.flink.util.Collector

//full join
object CoProcessFunctionExample {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //将两条流进行联合处理，对sensor_2和sensor_3进行开关操作
    val readings: KeyedStream[SensorReading, String] = env.addSource(new SensorSource)
      .keyBy(_.id)

    val filterSwitches: KeyedStream[(String, Long), String] = env.fromElements(
      ("sensor_2",10*1000L),
      ("sensor_3",60*1000L)
    ).keyBy(_._1)
    //变成两个不一样的流

    readings
      .connect(filterSwitches)
      .process(new MyCoPrecessFunction).print()

    env.execute("coProcessFunction example")


  }
  class MyCoPrecessFunction extends CoProcessFunction[SensorReading,(String, Long),String] {

    lazy val Openstate: ValueState[Boolean] =getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean](
        "timer",
        Types.of[Boolean]
      )
    )
  //keyBy了之后是没有
    override def processElement1(value: SensorReading, ctx: CoProcessFunction[SensorReading, (String, Long), String]#Context, out: Collector[String]): Unit = {
      if(Openstate.value){
        out.collect(value.toString)
      }
    }


    override def processElement2(value: (String, Long), ctx: CoProcessFunction[SensorReading, (String, Long), String]#Context, out: Collector[String]): Unit = {
      Openstate.update(true)
      val Timers = ctx.timerService().currentProcessingTime() + value._2
      ctx.timerService().registerProcessingTimeTimer(Timers)


    }
    override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      Openstate.update(false)
    }
  }
  }




