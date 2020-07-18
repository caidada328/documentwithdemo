package com.atguiug.flink.day06_day07

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.descriptors._

object TableAPITest2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val tableEnv = StreamTableEnvironment.create(env)
    //创建不同的执行环境
    //创建老版本的流处理环境 create可以传settings或者tableEnvConfig
    val settings  = EnvironmentSettings.newInstance()
      .useOldPlanner() //使用pom中的老planner
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env,settings)

    //创建老版本批处理的环境
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv = BatchTableEnvironment.create(batchEnv)

  /*  //创建新版本的流式处理环境
    val settings2 = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val newTableEnv = StreamTableEnvironment.create(env,settings2)
    //创建blink新版本的批处理环境  <批流统一>
    val bbSetings  = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val bbTableEnv = TableEnvironment.create(bbSetings)*/

    //2.从外部系统读取数据，在环境中注册表
     //2.2 连接到文件系统 (CSV 逗号分隔的)
    val filePath = "C:\\Scalaa\\flink\\src\\main\\resources\\source.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      //.withFormat(new OldCsv()) //如何读取数据之后的格式化方法
      .withFormat(new Csv())  //使用新版本的CSV
      .withSchema(new Schema() //定义表结构 ，并追加各个列的类型
    .field("id",DataTypes.STRING())
    .field("timestamp",DataTypes.BIGINT())
    .field("tempreture",DataTypes.DOUBLE()))
      .createTemporaryTable("myTable") //注册临时表


    //2.2连接到Kafka
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


    //3.表的查询
    //3.1 简单查询
    //3.1.1 huo获得表
    val sensorTable = tableEnv.from("myTable")
    val resultTable = sensorTable
      .select('id,'tempreture)
        .filter('id  === "sensor_1") //tableAPI特殊的写法
      //3.2 SQL简单查询
      val resultSqlTable = tableEnv.sqlQuery(
          //直接从注册的Table查询
          """
            |select id,tempreture
            |from myTable
            |where id = 'sensor_1'
          """.stripMargin
      )
      //3.3 做聚合操作
      val aggregateTable = sensorTable
        .groupBy('id)
          .select('id,'id.count as 'id_count)
      //3.4SQL 实现简单聚合
      val aggSqlTable = tableEnv.sqlQuery("select id, count(id) as count_id from myTable group by id")

      //因为这是聚合操作，就是在原有的数据的基础上修改，而不是追加，所以使用了toRetractStream方法 ， 就是更新模式
      aggregateTable.toRetractStream[(String,Long)].print()
      /*结果
      （true,(sensor_1,1)）
     （false,(sensor_1,1)）
     （true,(sensor_1,2)）
      true表示有效，更新
      false表示要删除的老数据
      *
      * */





      //将表转换为流来进行打印
     println(sensorTable.toAppendStream[(String, Long, Double)])
    val kafkaTable = tableEnv.from("kafkaTable")
    kafkaTable.toAppendStream[(String,Long,Double)]
    env.execute("tableAPiTest2")
  }


}
