package day3

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object AvgTeempFunc {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val source = env.addSource(new SensorSource)
    val resultStream = source.map(r =>(r.id,r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .process(new AvgTempFunc)

    println(resultStream)

    env.execute("process allWindowFunction job")
  }

  class AvgTempFunc extends ProcessWindowFunction[(String, Double), (String, Double), String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Double)], out: Collector[(String, Double)]): Unit = {

      val size = elements.size
      var sum = 0.0
      for(r <- elements){
        sum += r._2
      }
      out.collect((key,Math.floor(sum/size)))
    }
  }

}
