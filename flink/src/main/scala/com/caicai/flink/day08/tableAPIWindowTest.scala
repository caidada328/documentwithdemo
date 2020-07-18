package com.atguiug.flink.day08
import java.sql.Timestamp

import com.atguiug.flink.day01_03.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Over, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object tableAPIWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //在代码中定义时间语义 可以定义三种都有

    val inputStream: DataStream[String] = env.readTextFile("C:\\Scalaa\\flink\\src\\main\\resources\\source.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(data =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
    })//设置watermark
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(
                                     element: SensorReading
                                   ): Long = element.timestamp
    })
    //创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)
    //基于数据流，将流数据转换成一张表，   在TableApi  定义处理时间  'et.rowtime 是eventTime 就是从流处理API拿到的  这些时间语义必须是在末尾
    val sensorTble = tableEnv.fromDataStream(dataStream,'id,'timestamp as 'ts,'tempreture as 'tm,'ps.proctime)

    //3窗口操作
    //分3.1组窗口操作
    //3.1.1每十秒滚动一次，计算每类传感器温度的数量
    val groupedResultTable = sensorTble
        .window(Tumble over 10.seconds on 'ts as 'tw)
    //'tw.start,'tw.end方法
        .groupBy('id,'tw)
        .select('id,'id.count,'tw.end)

    groupedResultTable.toAppendStream[(String,Double,Timestamp)].print()
    //3.1.2Groupc窗口的SQL实现
    tableEnv.createTemporaryView("sensor",sensorTble)
    //类似tumble_end的方法还有
    /*tumble_start
    tumble_end 时间信息
    tumble_rowtime
    tumble_proctime
    *
    *
    * */
    val groupedsqlTable = tableEnv.sqlQuery(
      """
        |select
        |id,
        |count(id),
        |tumble_end(ts,interval '10' second)
        |from sensor
        |group by
        |id,
        |tumble(ts,interval '10' second)
      """.stripMargin
      )

    //3.2 Over窗口  对每个传感器每一行数据与前两行数据的平均温度
    //3.2.1  TableAPI
    val overResultTable = sensorTble
        .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'w )
        .select('id,'ts,'id.count over 'w, 'tempreture.avg over 'w)

    overResultTable.toAppendStream[Row].print()
    //3.2.2 Over窗口的Sql 实现

    val overSqlTable = tableEnv.sqlQuery(
      """
        |select id,ts,
        |count(id) over w, avg(tempreture) over w
        |from sensor
        |window w as (
        |partition by id order by ts
        |rows between 2 preceding and current row
        |)
      """.stripMargin)

    overSqlTable.toAppendStream[Row].print()




    groupedsqlTable.toAppendStream[(String,Double,Timestamp)].print()
    env.execute(" TableAPIWindow")
  }
}
