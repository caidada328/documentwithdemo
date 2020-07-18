package com.caicai.hotitems_analysis
import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.api.scala._

object TopNWithTableAPI {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

     val tableEnv = StreamTableEnvironment.create(env,settings)

    val dataTable = tableEnv.fromDataStream(dataStream,'itemId,'behavior,'timestamp as 'ts)

    val aggTable = dataTable.
      filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId,'sw)
      .select('itmId,'ItmId.count as 'cnt,'sw.end as 'windowEnd)

    tableEnv.createTemporaryView("agg",aggTable,'itemId,'cnt,'windowEnd)

    val resultTable = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |    select *,
        |      row_number() over (partition by windowEnd order by cnt desc) as row_num
        |    from agg
        |)
        |where row_num <= 5
      """.stripMargin)

    resultTable.toRetractStream[(Long, Long, Timestamp, Long)].print("result")

    env.execute("hot item with table api & sql")




  }

}
