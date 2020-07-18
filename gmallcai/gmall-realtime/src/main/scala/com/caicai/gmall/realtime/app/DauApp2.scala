import java.util.Date
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.caicai.gmall.common.Constant
import com.caicai.gmall.realtime.bean.StartupLog
import com.caicai.gmall.realtime.util.{MykafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
/*package com.caicai.gmall.realtime.app
import java.sql.Date
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.caicai.gmall.common.Constant
import com.caicai.gmall.realtime.bean.StartupLog
import com.caicai.gmall.realtime.util.{MykafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DauApp").setMaster("local[4]")

    val ssc = new StreamingContext(conf, Seconds(3))

    val sourceStream = MykafkaUtil.getKafkaStream(ssc,Constant.TOPIC_STARTUP)

    val startupLogStream = sourceStream.map(jsonStr => JSON.parseObject(jsonStr,classOf[StartupLog]))


    val firstStartupLogStream = startupLogStream.transform(rdd =>{
      val client = RedisUtil.getClient

      val key = Constant.TOPIC_STARTUP + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())

      val midss = client.smembers(key)
      client.close()

      val midsDB = ssc.sparkContext.broadcast(midss)

      rdd.filter(log => !midsDB.value.contains(log.mid))
          .map(log => (log.mid,log))
          .groupByKey()
          .map{
            case (_,it) => it.toList.sortBy(_.ts).head
          }
    })

    import org.apache.phoenix.spark._

    firstStartupLogStream.foreachRDD(rdd =>{
      rdd.foreachPartition(logIt =>{
       val client =  RedisUtil.getClient
        logIt.foreach(log =>{
          client.sadd(Constant.TOPIC_STARTUP + ":" + log.logDate,log.mid)
        })
        client.close()
      })
        rdd.saveToPhoenix(Constant.DAU_TABLE,
          Seq("MID","UID","APPID","AREA","OS","CHANNEL","LOGFILE","VERSION","TS","LOGDATE","LOGHOUR"),
          zkUrl = Some("hadoop102,hadoop103,hadoop104:2181"))

    })

    firstStartupLogStream.print(1000)
    ssc.start()
    ssc.awaitTermination()

  }

}*/

object DauApp2{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("DAuApp")
    val ssc = new StreamingContext(conf,Seconds(3))
    val sourceStream = MykafkaUtil.getKafkaStream(ssc,Constant.TOPIC_STARTUP)
    val startupLogStream = sourceStream.map(jsonstr => JSON.parseObject(jsonstr,classOf[StartupLog]))
    val firstStartupLogStream = startupLogStream.transform(rdd =>{

      val client = new Jedis("hadoop103",6379,60*1000)
      client.connect()
      val key = Constant.TOPIC_STARTUP + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val mids = client.smembers(key)
      client.close()
      val midsBD = ssc.sparkContext.broadcast(mids)

      rdd.filter( (log: StartupLog) => !midsBD.value.contains(log.mid))
          .map( (log: StartupLog) => (log.mid,log))
        .groupByKey()
          .map{
            case(_,it) => it.toList.minBy(_.ts)
          }
      rdd
    })
    import org.apache.phoenix.spark._

    firstStartupLogStream.foreachRDD( (rdd: RDD[StartupLog]) =>{
          rdd.foreachPartition( (logIt: Iterator[StartupLog]) => {
            val client = RedisUtil.getClient
            logIt.foreach( (log: StartupLog) =>{
              client.sadd(Constant.TOPIC_STARTUP + ":" + log.logDate, log.mid)
            })

            client.close()
            })

            rdd.saveToPhoenix(Constant.DAU_TABLE,
              Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
            zkUrl = Some("hadoop102,hadoop103,hadoop104:2181"))
          })

          firstStartupLogStream.print(1000)
          ssc.start()
          ssc.awaitTermination()
  }
}