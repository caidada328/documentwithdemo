package com.atguiug.flink.day06_day07
import com.atguiug.flink.day01_03.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}
//其它详见官网  、flink-docs-release-1.10/dev/table  有HIve/Hbase/Es/JDBC
//Kafka是只支持Append模式的，不支持update的，如果想使用这种类似表格的操作，可以选择ES,HBase等
//其中JDBC没有java或者scala的集成，所以需要纯的sql语句
object KafkaTableTest {
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


    /*
 需求 ：从Kafka输入 ，输出到Kafka
 *
 * */
    //定义一个Kafka数据源
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("bootstrap.servers","hadoop104:9092")
      .property("zookeeper.connect","hadoop104:2181,hadoop105:2181,hadoop106:2181"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("tempreture",DataTypes.DOUBLE()))
      .createTemporaryTable("kafkaTable")

    //做中间的转换操作
    val sensorTable : Table= tableEnv.from("kafkaTable")
    val resultTable = sensorTable
      .select('id,'temp,'ts)
      .filter('id  === "sensor_1")

    //定义一个KafkaSink
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sinkResult")
      .property("bootstrap.servers","hadoop104:9092")
      .property("zookeeper.connect","hadoop104:2181,hadoop105:2181,hadoop106:2181"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("temp",DataTypes.BIGINT())
        .field("ts",DataTypes.DOUBLE()))
      .createTemporaryTable("KafkaOutputTable")


    //将数据输出
    resultTable.insertInto("KafkaOutputTable")
    env.execute("KafkaTableTest")
  }


}
