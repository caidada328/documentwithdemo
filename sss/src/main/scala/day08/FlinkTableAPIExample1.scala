package day08

import day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._


object FlinkTableAPIExample1 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env,settings)

    val sensorTable = tableEnv.fromDataStream(stream,'id,'timestamp.rowtime as 'ts,'temprature as 'tamp)

    val resultStream = sensorTable.window(Tumble over 10.seconds on 'ts as 'w)
      .groupBy('id,'timestamp)
      .select('id,'id.count)
      .toRetractStream[(String,Long)]
      .print()

    env.execute("window table API job")
  }


}
