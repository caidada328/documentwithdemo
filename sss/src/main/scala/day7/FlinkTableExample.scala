package day7


import day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object FlinkTableExample {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
   env.enableCheckpointing()


    // 创建表环境, 使用blink planner

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    tableEnv
      .connect(new FileSystem().path("C:\\Scalaa\\sss\\src\\main\\resources\\source.txt"))
      .withFormat(new Csv()) // 按照csv文件格式解析文件
      .withSchema( // 定义表结构
        new Schema()
          .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")    // 创建临时表

    val sensorTable: Table = tableEnv.from("inputTable") // 将临时表inputTable赋值到sensorTable
    // 使用table api
    val resultTable: Table = sensorTable
      .select("id, temperature") // 查询`id`, `temperature` => (String, Double)
     .filter('id  === "sensor_1") // 过滤

    resultTable
        .toAppendStream[(String, Double)] // 追加流
      .print()

    // 使用flink sql的方式查询
    val resultSqlTable: Table = tableEnv
      .sqlQuery("select id, temperature from inputTable where id ='sensor_1'")

    resultSqlTable
      .toAppendStream[(String, Double)] // 追加流
     // .print()

    // 将DataStream转换成流表

    val stream = env.addSource(new SensorSource)

    val table = tableEnv.fromDataStream(stream, 'id, 'timestamp as 'ts, 'temperature as 'temp)
    table
      .select('id, 'temp)
      .toRetractStream[(String, Double)]
      //.print()

    env.execute()
  }
}