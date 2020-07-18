package day2

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer011}

object KafkaExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val props2 = new Properties()
    props2.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserialization")
    props2.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserialization")
    props2.put("auto.offset.reset","latest")
    props2.put("broker.list","hadoop104:9092,hadoop105:9092,hadoop106:9092")

    val stream = env
      .addSource(new FlinkKafkaConsumer[String](
          "test",
          new SimpleStringSchema(),
          props2
        )
      )

 /*   stream.addSink(new FlinkKafkaProducer011[String](
        "localhost:9092",
        "test",
        new SimpleStringSchema()
      )
    )*/

    stream.print()
    env.execute()
  }


}
