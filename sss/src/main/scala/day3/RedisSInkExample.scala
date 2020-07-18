package day3

import org.apache.flink.api.common.restartstrategy.RestartStrategies.FallbackRestartStrategyConfiguration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSInkExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setRestartStrategy(new FallbackRestartStrategyConfiguration)

    val inputStream = env.addSource(new SensorSource)
   val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop104").setPort(6379).build()
    val Mapper = new RedisMapper[SensorReading] {
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET,"sensor_info")}

      override def getKeyFromData(data: SensorReading): String = data.id

      override def getValueFromData(data: SensorReading): String = data.temperature.toString
    }
    inputStream.addSink(new RedisSink[SensorReading](conf,Mapper))

    env.execute("RedisSink")
  }
}