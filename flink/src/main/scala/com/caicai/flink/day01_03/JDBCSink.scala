package com.atguiug.flink.day01_03

import java.sql.{Connection, DriverManager, PreparedStatement}


import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
object JDBCSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("C:\\Scalaa\\flink\\src\\main\\resources\\source.txt")
    val dataStream = inputStream.map(
      data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })

    dataStream.addSink(new MyJdbcSink)

    env.execute("jdbcsink user define")
  }

  class MyJdbcSink() extends RichSinkFunction[SensorReading]{
    //先定义sql连接，以及预编译语句

    var conn : Connection = _
    var insertStmt:PreparedStatement =_
    var updateStmt:PreparedStatement =_

      override def open(parameters: Configuration): Unit = {
     //表需要提前建好
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root","")
      insertStmt = conn.prepareStatement("insert into temp(tempreture,id) values(?,?)")
      updateStmt = conn.prepareStatement("update temp set tempreture = ? where sensor = ?")
    }

  //执行任务
    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
     //传入参数 做插入操作
      updateStmt.setDouble(1,value.tempreture)
      updateStmt.setString(2,value.id)
      updateStmt.execute()
    //判断是否是更新操作,不是更新操作，就执行插入操作
      if(updateStmt.getUpdateCount == 0){
        insertStmt.setDouble(1,value.tempreture)
        insertStmt.setString(2,value.id)
        insertStmt.execute()
      }
    }

    override def close(): Unit ={
      //关闭资源
      insertStmt.close()
      updateStmt.close()
      conn.close()
    }
  }

}
