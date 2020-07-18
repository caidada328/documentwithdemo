package day06

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UpdateWindowResultWithLateEle {
  def main(args: Array[String]): Unit = {
    val env =StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //写在配置文件或者写死在webUI上不显示
   // env.getConfig.setGlobalJobParameters()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("hadoop104",9999,'\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0),arr(1).toLong*1000)

      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
        override def extractTimestamp(element: (String, Long)): Long = element._2
      })
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(5))
      .process(new UpdatingWindowCountFunction)

    stream.print()
    env.execute()
  }

  //全窗口函数，是当窗口达到后才计算，满了后才执行方法
  class UpdatingWindowCountFunction extends ProcessWindowFunction[(String,Long),String,String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      val count = elements.size
      //windowTime，这里的状态量仅仅是当前可见。
      val isUpdate: ValueState[Boolean] = context.windowState.getState(new ValueStateDescriptor[Boolean]("isupdate",classOf[Boolean]))
      if(!isUpdate.value()){
        out.collect("当水位线超过窗口结束时间的时候，窗口第一次触发计算！就会触发操作，来了" + count + "个")
        isUpdate.update(true)
      }else{//如果数据不是实际的窗口，而是别的窗口，也会触发计算。
        out.collect("一共迟到了"+ count + "个")
      }
    }
  }
}
