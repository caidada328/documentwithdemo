package com.atguiug.flink.day06_day07

import com.atguiug.flink.day01_03.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object OutputTableTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("C:\\Scalaa\\flink\\src\\main\\resources\\source.txt")
    val dataStream = inputStream.map(
      data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })

    val settings  = EnvironmentSettings.newInstance()
      .useOldPlanner() //使用pom中的老planner
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env,settings)
    //可以更改位置，可以重命名
    val sensorTable : Table= tableEnv.fromDataStream(dataStream,'id,'tempreture as 'temp,'timestamp as 'ts)
    //定义一张输出表就是一个TableSink
    tableEnv.connect(new FileSystem().path("C:\\Scalaa\\flink\\src\\main\\resources\\output.txt"))
        .withFormat(new Csv())
        .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("temp",DataTypes.DOUBLE())
        .field("ts",DataTypes.BIGINT()))
        .createTemporaryTable("outputTable")
    //sensorTable.printSchema()

    val resultTable = sensorTable
      .select('id,'temp,'ts)
      .filter('id  === "sensor_1")

    resultTable.insertInto("outputTable")


  //  resultSqlTable.insertInto("outputTable")

    val aggregateTable = sensorTable
      .groupBy('id)
      .select('id,'id.count as 'id_count)
   // aggregateTable.insertInto("outputTable")

    sensorTable.toAppendStream[(String,Double,Long)].print()

    env.execute("OutputTest1")

  }
}
