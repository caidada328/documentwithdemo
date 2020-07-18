package day06

import day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


object ListStateExample {
  def main(args: Array[String]): Unit = {
    val env =StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource( new SensorSource)
      .filter(_.id.equals("sensor_1"))
      .keyBy(_.id)
      .process(new MyKeyedProcess)
      .print()

    val ttlConfig=StateTtlConfig.newBuilder(Time.seconds(1))
      .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build()

    env.execute("listState Job")
  }

  class MyKeyedProcess extends KeyedProcessFunction[String,SensorReading,String] {
    var listState :ListState[SensorReading] = _

    var timerTs:ValueState[Long] = _

    //几个并行度就会调用几次
    override def open(parameters: Configuration): Unit = {
      listState = getRuntimeContext.getListState(new ListStateDescriptor[SensorReading]("mylist-state",classOf[SensorReading]))

      timerTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time",classOf[Long]))

    }

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      listState.add(value)
      if(timerTs.value() == 0L || listState.get() == null){
        val ts = ctx.timerService().currentProcessingTime()+10*1000L
        ctx.timerService().registerProcessingTimeTimer(ts)
        timerTs.update(ts)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      val list: ListBuffer[SensorReading] = ListBuffer()
      import scala.collection.JavaConversions._
      for (r <- listState.get()) {
        list += r
      }
      listState.clear()
      out.collect("列表状态变量的数据数量："+ list.size)
      timerTs.clear()
    }
  }
}
