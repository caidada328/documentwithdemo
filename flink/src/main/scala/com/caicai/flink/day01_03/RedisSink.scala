package com.atguiug.flink.day01_03

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSink {
 def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //env.setRestartStrategy()

    val inputStream = env.readTextFile("C:\\Scalaa\\flink\\src\\main\\resources\\source.txt")
    val dataStream = inputStream.map(
      data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })
    //定义一个redisconf
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop104")
      .setPort(6379)
      .build()
    //dingyiyigeRedisMapper
  val myMapper = new RedisMapper[SensorReading] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
    }

    override def getKeyFromData(data: SensorReading): String = data.id

    override def getValueFromData(data: SensorReading): String = data.tempreture.toString
  }

  dataStream.addSink(new RedisSink[SensorReading](conf,myMapper))

  env.execute("redisSink")
}



}
