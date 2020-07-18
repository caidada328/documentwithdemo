package day04

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object WatermarkBroadcast {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rawStream = env.socketTextStream("hadoop104",9999,'\n')
    val stream1 = rawStream.map(line =>{
      val array = line.split("")
      (array(0),array(1).toLong*1000)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(2)) {
      override def extractTimestamp(element: (String, Long)): Long = element._2
    })

    val rawStream2 = env.socketTextStream("hadoop104",9998,'\n')
    val stream2 = rawStream2.map(line =>{
      val array = line.split("")
      (array(0),array(1).toLong*1000)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.milliseconds(4000)) {
      override def extractTimestamp(element: (String, Long)): Long = element._2
    })

    val result = stream1.keyBy(_._1).connect(stream2.keyBy(_._1)).process(new CoPress)

    result.print()
    env.execute("watermark test")
  }
  class CoPress extends CoProcessFunction[(String,Long),(String,Long),String] {
    override def processElement1(value: (String, Long), ctx: CoProcessFunction[(String, Long), (String, Long), String]#Context, out: Collector[String]): Unit = {
      out.collect(" 第一条数据流的水位线是" + ctx.timerService().currentWatermark()+"")
    }

    override def processElement2(value: (String, Long), ctx: CoProcessFunction[(String, Long), (String, Long), String]#Context, out: Collector[String]): Unit = {
      out.collect(" 第二条数据流的水位线是" + ctx.timerService().currentWatermark()+"")
    }
  }
}