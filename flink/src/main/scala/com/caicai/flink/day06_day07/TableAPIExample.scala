package com.atguiug.flink.day06_day07

import com.atguiug.flink.day01_03.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/*在集群环境下执行TableAPI需要拥有jar包
1.flink-table-blink
2.flink-table
*
* */
object TableAPIExample {
  def main(args: Array[String]): Unit = {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("C:\\Scalaa\\flink\\src\\main\\resources\\source.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(data =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
    })
   //创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)
    //基于数据流，将流数据转换成一张表，
    val datatable = tableEnv.fromDataStream(dataStream)
    //调用TableAPI,得到转换结果
   /* val resultTable = datatable
      .select("id,tempreture")
      .filter("id == 'sensor_1'")*/
    //一般只能打印出组织架构，不能打印表，需要将其转换为流
   // resultTable.printSchema()
    //需要引入隐式转换import org.apache.flink.table.api.scala._
  /*  val resultStream = resultTable.toAppendStream[(String,Double)]
    resultStream.print()*/

    //直接使用SQL语句来查询
    val table = tableEnv.sqlQuery("select id,tempreture from " + datatable + " where id = 'sensor_1'")
    //对于上面两行代码还可以这样写
    /*tableEnv.createTemporaryView("datatable",datatable)
    val table:Table = tableEnv.sqlQuery("select id,tempreture from  datatable where id = 'sensor_1'")*/
    table.printSchema()
    println(table.toAppendStream[(String, Double)])
    env.execute("table example")
  }
}
