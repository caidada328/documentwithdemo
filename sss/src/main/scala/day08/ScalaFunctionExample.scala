package day08

import day2.SensorSource
import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction


object ScalaFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)

    // 表相关代码
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env,settings)

    val hashCode = new HashCode(10)

    val dataTable = tableEnv
      .fromDataStream(stream,'id,'timestamp.rowtime as 'ts,'temperature as 'temp)

    dataTable.select('id,hashCode('id))
      .toAppendStream[(String,Int)]
     // .print()

    //flink-sql模式
    tableEnv.registerFunction("hashcode",new HashCode(10))
    tableEnv.createTemporaryView("t",dataTable,'id)
    tableEnv.sqlQuery(
      """
        |select id ,hashCode(id) from t
      """.stripMargin)
        .toAppendStream[(String,Int)]
        .print()
    env.execute("scalarFunction job")

  }

  class HashCode(val factor:Int) extends ScalarFunction{
    def eval(s:String):Int={
      s.hashCode()*factor
    }
  }

}
