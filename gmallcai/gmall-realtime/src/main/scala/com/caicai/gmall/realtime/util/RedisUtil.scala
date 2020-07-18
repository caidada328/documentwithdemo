package com.caicai.gmall.realtime.util

import redis.clients.jedis.Jedis

/**
 * Author caicai
 * Date 2020/3/30 14:17
 */
object RedisUtil {
    val host: String = ConfigUtil.getProperty("redis.host")
    val port: Int = ConfigUtil.getProperty("redis.port").toInt
    
    def getClient: Jedis = {
        val client: Jedis = new Jedis(host, port, 60 * 1000)
        client.connect()

        client
    }
}

/*
object RedisUtil2 {
  val host = ConfigUtil.getProperty("redis.host")
  val port = ConfigUtil.getProperty("redis.host").toInt

  def getClient={
    val client = new Jedis(host,port,60*1000)
    client.connect()
    client
  }

}*/
