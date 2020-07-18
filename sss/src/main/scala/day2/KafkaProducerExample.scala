package day2

import java.util.Properties


object KafkaProducerExample {
  def main(args: Array[String]): Unit = {

  }

  def write2Kafka (topic:String) :Unit ={
    val props = new Properties()

    //基本参数 key value 序列化
    props.put("key.serializer","org.apache.kafka.common.serializaiton.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serializaiton.StringSerializer")
    props.put("bootstrap.servers","hadoop104:9092,hadoop105:9092,hadoop106:9092")

//    val producer = new KafkaProducer[String,String](props)
//    val record = new ProducerRecord[String,String](topic,"hello world")
//    producer.send(record)
//    producer.close()
  }



}
