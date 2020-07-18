package com.caicai.gmall.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.caicai.gmall.realtime.util.MykafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import com.caicai.gmall.common.{Constant, ESUtil}
import com.caicai.gmall.realtime.bean.{AlertInfo, EventLog}
import org.apache.spark.streaming.dstream.DStream

import scala.util.control.Breaks.{break, breakable}

object AlertApp2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("AlertApp")

    val ssc = new StreamingContext(conf,Seconds(6))

    val sourceStream = MykafkaUtil
      .getKafkaStream(ssc,Constant.TOPIC_EVENT)
      .window(Minutes(5),Seconds(6))
    val eventLogStream = sourceStream
      .map(s => JSON.parseObject(s,classOf[EventLog]))

    val eventlogGrouped = eventLogStream.map(
      eventLog =>(eventLog.mid,eventLog)
    ).groupByKey

    val altertInfoStream: DStream[(Boolean, AlertInfo)]  = eventlogGrouped.map{
      case (mid,eventLogs: EventLog) =>
        val uidSet = new util.HashSet[String]()
        val itemSet = new util.HashSet[String]()
        val eventList = new util.ArrayList[String]()
        var isClickItem = false
        breakable{
          eventLogs.foreach(log =>{
            eventList.add(log.eventId)
            log.eventId match{
              case "coupon" =>
                uidSet.add(log.uid)
              case "clickItem " =>
                isClickItem = true
                break
              case _ =>
            }
          })
        }
        (uidSet.size() >= 3 && !isClickItem, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))

    }
    altertInfoStream.filter(_._1).map(_._2).foreachRDD(rdd =>{
      rdd.foreachPartition( (alterInfos: Iterator[AlertInfo]) =>{
        val data = alterInfos.map(info =>(info.mid + ":" +info.ts /1000 /60, info))
        val c = 111

        ESUtil.insertBulk(Constant.INDEX_ALTER,data)
      })
    })
    altertInfoStream.print(10000)

    ssc.start()
    ssc.awaitTermination()
  }
}
