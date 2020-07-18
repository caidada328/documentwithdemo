package day08

import day08.TableFunctionExample.Split
import day2.SensorSource
import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

object FlinkTableAPIExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    val stream = env.addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)


    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env,settings)

    val dataTable = tableEnv
      .fromDataStream(stream,'id ,'timestamp.rowtime as 'ts,'temperature as 'temp)

    tableEnv.sqlQuery("SELECT id ,COUNT(id) FROM " + dataTable + "GROUP BY id,tumble(ts,TUMBLE '10' second)")
      .toRetractStream[(String,Long)]
      .print()

    env.execute()
  }

}
