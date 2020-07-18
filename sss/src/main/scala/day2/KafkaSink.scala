package day2

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object KafkaSink {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("C:\\Scalaa\\flink\\src\\main\\resources\\source.txt")
    val dataStream = inputStream.map(
      data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble).toString
      }
    )

   // dataStream.addSink(new FlinkKafkaProducer011[String]("hadoop104:9092","flink2Kafka_sink",new SimpleStringSchema()))
    env.execute("翻车了，老师")
  }
}
