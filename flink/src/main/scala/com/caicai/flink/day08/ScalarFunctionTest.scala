package com.atguiug.flink.day08
import com.atguiug.flink.day01_03.SensorReading
import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.functions.timestamps._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object ScalarFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //在代码中定义时间语义 可以定义三种都有

    val inputStream: DataStream[String] = env.readTextFile("C:\\Scalaa\\flink\\src\\main\\resources\\source.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(data =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
    })//设置watermark
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(
                                     element: SensorReading
                                   ): Long = element.timestamp
    })



    //创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)
    //基于数据流，将流数据转换成一张表，   在TableApi  定义处理时间  'et.rowtime 是eventTime 就是从流处理API拿到的  这些时间语义必须是在末尾
    val sensorTble = tableEnv.fromDataStream(dataStream,'id,'timestamp as 'ts,'tempreture as 'tm,'ps.proctime)

  //如何使用这个标量函数呢
    //1.TableAPI使用
    //1.1创建一个UDF实例
    val hashcode = new Hashcode(2)

    //1.2使用这个实例，并传入参数
    val resultTable = sensorTble
        .select('id,'ts,hashcode('id)) //这个‘id 就是对应的传入eval方法的str

    resultTable.toAppendStream[(String,Long,Int)].print()
    //2.SQLDDL使用
    //1.需要使用tableEnvironment注册
    tableEnv.registerFunction("hashcodeF",hashcode)
    //2.创建一个临时表
    tableEnv.createTemporaryView("sensor",sensorTble)
    //3.使用注册好的函数
    val resultSQLTable = tableEnv.sqlQuery("select id,ts,hashcodeF(id) from sensor")

    resultSQLTable.toAppendStream[Row].print()
    env.execute("ScalarFunctionTest")
  }

}


//实现自定义标量函数

class Hashcode(factor:Int) extends ScalarFunction{
//必须要实现eval方法，参数是当前传入的字段，

def eval(str:String) :Int ={
    str.hashCode*factor
  }
}