package com.caicai.gmall.realtime.util




import kafka.serializer.StringDecoder

import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Author caicai
 * Date 2020/3/30 11:14
 */
object MykafkaUtil {
    
    val params = Map[String, String](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> ConfigUtil.getProperty("kafka.servers"),
        ConsumerConfig.GROUP_ID_CONFIG-> ConfigUtil.getProperty("kafka.group.id")
    )
    
    def getKafkaStream(ssc: StreamingContext, topic: String, otherTopics: String*) = {
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            params,
            (otherTopics :+ topic).toSet
        ).map(_._2)
    }
}



/*
object MyKafkaUtil2{

    val params = Map[String, String](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> ConfigUtil.getProperty("kafka.servers"),
        ConsumerConfig.GROUP_ID_CONFIG-> ConfigUtil.getProperty("kafka.group.id")
    )
//这个不完善，需要利用Mysql的事务机制完成，使得Spark从Kafka消费数据不丢失数据也不造成数据重复
    def getKafkaStream(ssc:StreamingContext,topic:String,otherTopics:String*) ={
        KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
            ssc,
            params,
            (otherTopics :+ topic).toSet
        ).map(_._2)
    }
}

object MykafkaUtil3{
  val params1 = Map[String,String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> ConfigUtil.getProperty("kafka.servers"),
      ConsumerConfig.GROUP_ID_CONFIG -> ConfigUtil.getProperty("kafka.group.id")
  )

  def getKafkaStream1(ssc:StreamingContext,topic:String,otherTopics:String*) ={
    KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,
        params1,
        ( otherTopics :+ topic).toSet,
    ).map(_._2)
  }
}*/
