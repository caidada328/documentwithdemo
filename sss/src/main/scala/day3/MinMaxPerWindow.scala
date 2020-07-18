package day3

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object MinMaxPerWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val source = env.addSource(new SensorSource)
    source.keyBy(_.id)
        .timeWindow(Time.seconds(5))
        .process(new HighAndLowTempPerWIndow)
        .print()

    env.execute("MinMaxPerWindow")
  }

  class HighAndLowTempPerWIndow extends ProcessWindowFunction[SensorReading,MinMaxTemp,String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[MinMaxTemp]): Unit = {

      val temps = elements.map(_.temperature)
      val windowEnd = context.window.getEnd
      out.collect(MinMaxTemp(key, temps.min ,temps.max ,windowEnd))
    }
  }


}


