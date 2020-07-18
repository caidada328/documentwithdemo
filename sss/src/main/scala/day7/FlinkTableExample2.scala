package day7

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}


object FlinkTableExample2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val setting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env,setting)

    tableEnv.connect(new FileSystem().path("C:\\Scalaa\\sss\\src\\main\\resources\\source.txt"))
      .withFormat(new Csv)
      .withSchema(new Schema()
      .field("id",DataTypes.STRING())
      .field("timestamp",DataTypes.BIGINT())
      .field("tempreture",DataTypes.DOUBLE()))
      .createTemporaryTable("sensorTable")

    val sensorTable = tableEnv.from("seneorTable")

/* val resultStream = sensorTable
      .select('id,'ts as 'timestamp)
        .toAppendStream[(String,Double)]
        .print()*/

     val resultStream2 =tableEnv.sqlQuery("select * from sensorTable where  id='sensor_1' ")
       .toRetractStream[(String,Long,Double)].print()

    env.execute("Table API Test")

  }


}
