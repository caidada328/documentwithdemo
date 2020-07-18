package com.atguiug.flink.day01_03.day2

import org.apache.flink.streaming.api.scala._
import com.atguiug.flink.day01_03.day2.SensorSource
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSInkExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.addSource(new SensorSource)
    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop104").setPort(6379).build()
    val MyMapper = new RedisMapper[SensorReading] {
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")

      }

      override def getKeyFromData(data: SensorReading): String = data.id

      override def getValueFromData(data: SensorReading): String = data.temperature.toString
    }
    inputStream.addSink(new RedisSink[SensorReading](conf,MyMapper))

    env.execute("Sink2Redis")
  }
}