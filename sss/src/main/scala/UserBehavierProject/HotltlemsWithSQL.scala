package UserBehavierProject



import UserBehavierProject.TopHotItems.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.types.Row


object HotltlemsWithSQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("C:\\Scalaa\\sss\\src\\main\\resources\\UserBehavior.csv")
    val datastream = env
      .readTextFile("C:\\Scalaa\\sss\\src\\main\\resources\\UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toLong, arr(3), arr(4).toLong * 1000L)
      })
      .assignAscendingTimestamps(_.timestamp)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    tableEnv.createTemporaryView("data_table", datastream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    val resultTable = tableEnv.sqlQuery(
      """
        |select *
        |from(
        |     select *,
        |      row_number() over (partition by windowEnd order by cnt desc) as row_num
        |      from(select itemId,
        |      count(itemId) as cnt,
        |      HOP_END(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as windowEnd
        |    from data_table
        |    where behavior = 'pv'
        |    group by HOP(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) ,itemId
        |     )
        |)
        |where row_num <= 3
      """.stripMargin)
        .toRetractStream[Row]
        .filter(_._1 == true)
        .print()

    env.execute()
  }
}
