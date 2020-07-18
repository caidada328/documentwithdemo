package day3

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests



object ESSinkExample {




    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      val sourceStream = env.addSource(new SensorSource)
      var httpHosts = new util.ArrayList[HttpHost]()
      httpHosts.add(new HttpHost("haoop104",9200))
      val esSInkBuilder = new ElasticsearchSink.Builder[SensorReading](
        httpHosts,
        new ElasticsearchSinkFunction[SensorReading] {
          override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
            var source = new util.HashMap[String,String]()
            source.put("source",element.toString)

            Requests.indexRequest().index("sensor").`type`("ook").source(source)
          }
        }
      )

      esSInkBuilder.setBulkFlushMaxActions(10)
      sourceStream.addSink(esSInkBuilder.build())
      env.execute("Sink3ES job")
    }
}
