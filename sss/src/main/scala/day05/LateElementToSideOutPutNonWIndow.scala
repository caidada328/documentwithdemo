package day05

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object LateElementToSideOutPutNonWIndow {
  def main(args: Array[String]): Unit = {
    val env  = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val readings = env.socketTextStream("hadoop104",9999,'\n')
      .map(line =>{
        val arr = line.split(" ")
        (arr(0),arr(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)
      .process(new LateToSideOutpt)

    readings.print()
    readings.getSideOutput(new OutputTag[String]("lateness"))
  }

  class LateToSideOutpt extends ProcessFunction[(String,Long),String] {
    val lateReadingOutput = new OutputTag[String]("lateness")
    override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), String]#Context, out: Collector[String]): Unit = {
      if(value._2  < ctx.timerService().currentWatermark()){
        ctx.output(lateReadingOutput,"迟到事件来了")
      }
      else{

       out.collect("此事件没有迟到")

      }
    }



  }
}
