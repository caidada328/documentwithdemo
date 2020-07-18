package com.atguiug.flink.day01_03


import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._

object transformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val inputStreamFromFile:DataStream[String] = env.readTextFile("C:\\Scalaa\\flink\\src\\main\\resources\\source.txt")

    val dataStream: DataStream[SensorReading] = inputStreamFromFile.map(
      data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim,dataArray(1).trim.toInt,dataArray(2).trim.toDouble)
      }
    )
      .keyBy(data => data.id )
   //.keyBy(0)
        //.keyBy("id")
      //  .keyBy( new MyIDSelector)
      //  .sum("tempreture")
       // .min("tempreture")
      //  .reduce((a,b) => SensorReading(a.id,a.timestamp.max(b.timestamp),a.tempreture.min(b.tempreture)))
    //一直在实时更新，可以最后取得最新的结果，而且是符合更新使用reduce
        .reduce(new MyReduce)

    /*SensorReading("sensor_1",1547718199,35.8)
    SensorReading("sensor_1",1547718199,30.0)
    SensorReading("sensor_2",1547710000,29.0)
    SensorReading("sensor_2",1547711111,29.0)*/
    //分流
    val splitStream1 = dataStream.split(data =>{
      if(data.tempreture > 31) Seq("high")
      else Seq("low")
    })

    val highTempretureStream = splitStream1.select("high")
    val lowTempretureStream = splitStream1.select("low")
    val allTempretureStream = splitStream1.select("low","high")

   //合流
    val WarningStream :DataStream[(String,Double,String)] = highTempretureStream.map(
      data =>(data.id,data.tempreture,"high tempreture")
    )
    val ComfortStream:DataStream[(String,Double,String)] = lowTempretureStream.map(
      data =>(data.id,data.tempreture,"good")
    )
    val connectionStream = highTempretureStream.connect(dataStream)

    val unionStream = highTempretureStream.union(lowTempretureStream)


    dataStream.print()
    env.execute("transformTest")
  }
}
class MyIDSelector extends KeySelector[SensorReading,String]{
  override def getKey(value: SensorReading): String = value.id
}

class MyReduce extends ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading ={
    SensorReading(value1.id,value1.timestamp.min(value2.timestamp),value1.tempreture.max(value2.tempreture))
  }
}