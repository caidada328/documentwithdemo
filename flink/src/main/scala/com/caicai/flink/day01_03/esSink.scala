package com.atguiug.flink.day01_03

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.{Request, Requests}

object esSink {
  /*def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("C:\\Scalaa\\flink\\src\\main\\resources\\source.txt")
    val dataStream = inputStream.map(
      data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toInt,dataArray(2).toDouble)
      })

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop104",9200))


    val esSInkFunc = new ElasticsearchSinkFunction[SensorReading] {
      override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        //包装一个个数据
        val dataSource = new util.HashMap[String,String]()
        dataSource.put("sensor_id",element.id)
        dataSource.put("temp",element.tempreture.toString)
        dataSource.put("ts",element.timestamp.toString)

        //创建一个index request

        val indexRequest = Requests.indexRequest()
          .index("sensor_index")
          .`type`("sensor_type")
          .source(dataSource)

        indexer.add(indexRequest)
        println(element + "saved to redis successfully")
      }
    }

    dataStream.addSink(new ElasticsearchSink.Builder[SensorReading](httpHosts,esSInkFunc).build())

    env.execute("sinkToES job")
  }*/

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val datastream = env.addSource(new MySensorSource)
    val httpHost = new util.ArrayList[HttpHost]()
   httpHost.add(new HttpHost("hadoop104",9200))
    val esSInkFunc = new ElasticsearchSinkFunction[SensorReading] {
      override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        //包装一个个数据
        val dataSource = new util.HashMap[String,String]()
        dataSource.put("sensor_id",element.id)
        dataSource.put("temp",element.tempreture.toString)
        dataSource.put("ts",element.timestamp.toString)

        //创建一个index request

        val indexRequest = Requests.indexRequest()
          .index("sensor_index")
          .`type`("sensor_type")
          .source(dataSource)

        indexer.add(indexRequest)
        println(element + "saved to redis successfully")
      }
    }

    datastream.addSink(new ElasticsearchSink.Builder[SensorReading](httpHost,esSInkFunc).build())

    env.execute("sinkToES job")
  }
}
