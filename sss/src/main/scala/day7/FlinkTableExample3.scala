package day7

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}


object FlinkTableExample3 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env,settings)

    tableEnv.connect( new FileSystem().path("C:\\Scalaa\\sss\\src\\main\\resources\\source.txt"))
      .withFormat(new  Csv)
      .withSchema(new Schema()
      .field("id",DataTypes.SMALLINT())
      .field("timestamp",DataTypes.BIGINT())
      .field("tempreture",DataTypes.DOUBLE()))
      .createTemporaryTable("sensorTable")


    val sensorTable = tableEnv.from("sensorTAble")

    val resultStreaming2 = tableEnv.sqlQuery("select * from sensorTable where id = 'sensor_1'")
      .toRetractStream[(String,Long,Double)].print()

  env.execute("fxxk job")
  }

}
