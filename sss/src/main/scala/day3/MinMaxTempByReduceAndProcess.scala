package day3

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object MinMaxTempByReduceAndProcess {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)
    stream.map(r => (r.id,r.temperature,r.timestamp))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .reduce((r1:(String,Double,Long),r2:(String,Double,Long)) => {(r1._1,r1._2.min(r2._2),(r1._3.max(r2._3)).toLong)}
        ,
        new WindowResult
      )
    env.execute("MinMaxTempByReduceAndProcess job")
  }


  class WindowResult extends ProcessWindowFunction[(String,Double,Long),MinMaxTemp,String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Double, Long)], out: Collector[MinMaxTemp]): Unit = {

      val temp = elements.head
      out.collect(MinMaxTemp(temp._1,temp._2,temp._3,context.window.getEnd))
    }
  }
}
