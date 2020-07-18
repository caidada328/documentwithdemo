package day08

import day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object AggregateFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource).filter(_.id.equals("sensor_1"))

    val setttings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, setttings)

    val table = tableEnv.fromDataStream(stream, 'id, 'timestamp as 'ts, 'temperature as 'temp)

    //实例化udf函数  tableAPI
    val avgTemp = new AvgTemp
    table.groupBy('id)
      .aggregate(avgTemp('temp) as 'avgTemp)
      .select('id,'avgTemp)
      .toRetractStream[Row]
      .print()

    //flink-sql实现


    env.execute("AgagregateFunctionTableAPI job")


  }

  class AvgTempAcc{
    var sum:Double = 0
    var count:Int = 0
  }

  class AvgTemp extends AggregateFunction[Double,AvgTempAcc] {
    override def createAccumulator(): AvgTempAcc = new AvgTempAcc

    def accumulate(acc:AvgTempAcc,temp:Double):Unit ={
      acc.sum += temp
      acc.count += 1
    }

    override def getValue(accumulator: AvgTempAcc): Double = accumulator.sum / accumulator.count
  }
}
