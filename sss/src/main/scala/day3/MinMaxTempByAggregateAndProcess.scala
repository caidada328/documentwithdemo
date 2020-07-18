package day3

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object MinMaxTempByAggregateAndProcess {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)

    val resultStream = stream.keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new Agg,new AggResult)
      .print()

    env.execute("Singlewindow and stagewindow integration")
  }

  class Agg extends  AggregateFunction[SensorReading,(String,Double,Double),(String,Double,Double)] {
    override def createAccumulator(): (String, Double, Double) = ("", Double.MaxValue, Double.MinValue)

    override def add(value: SensorReading, accumulator: (String, Double, Double)): (String, Double, Double) =
      (value.id, value.temperature.min(accumulator._2), value.temperature.max(accumulator._3))

    override def getResult(accumulator: (String, Double, Double)): (String, Double, Double) = accumulator

    override def merge(a: (String, Double, Double), b: (String, Double, Double)): (String, Double, Double) =
      (a._1, a._2.min(b._2), a._3.max(b._3))
  }

  class AggResult extends WindowFunction[(String, Double, Double), MinMaxTemp, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[(String, Double, Double)], out: Collector[MinMaxTemp]): Unit = {

      val minMax = input.head
      out.collect(MinMaxTemp(key,minMax._2,minMax._3,window.getEnd))
    }
  }
}
