package day08


import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row


object TableFunctionExample {
  def main(args: Array[String]): Unit = {

    val env =StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.fromElements("hello#world","cai#haha")

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env,settings)
    val dataTable = tableEnv.fromDataStream(stream,'s)
    //用于TableAPI的使用,使用TableAPI来操作
 /*   val split = new Split("#")
    dataTable.joinLateral(split('s) as ('a,'b))
      .select('s,'a,'b)
      .toAppendStream[(String,String,Int)]
      .print()*/

    //使用flink-sql 来操作


    tableEnv.registerFunction("split",new Split("#"))
    tableEnv.createTemporaryView("t",dataTable)

    tableEnv.sqlQuery(
      """
        |select s,a,b from
        |t
        |left join lateral table(split(s)) as T(a,b)
        |on true
      """.stripMargin)
        .toAppendStream[Row]
        .print()

    env.execute("aa")

  }

  //官方实现
  class Split(seprator:String) extends TableFunction[Row]{

    def eval(str:String):Unit={

      str.split(seprator).foreach(t =>{
        val row = new Row(2)
        row.setField(0,t)
        row.setField(1,t.length)
        collect(row)
      })

    }

    override def getResultType: TypeInformation[Row] = {
      Types.ROW(Types.STRING,Types.INT)
    }
  }
//老师实现
 /* class Split(separator: String) extends TableFunction[(String, Int)] {
    def eval(str: String): Unit = {
      str.split(separator).foreach(word => collect((word, word.length)))
    }
  }*/

}
