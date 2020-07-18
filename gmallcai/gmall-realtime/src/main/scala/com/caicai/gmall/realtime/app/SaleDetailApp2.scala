package com.caicai.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.caicai.gmall.common.Constant

import com.caicai.gmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.caicai.gmall.realtime.util.{MykafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis


object SaleDetailApp2 {


  def saveToRedis(client:Jedis,key:String,value:AnyRef):Unit={
    import org.json4s.DefaultFormats
    val json = Serialization.write(value)(DefaultFormats)

    client.setex(key,30*60,json)
  }

  def cacheOrderInfo(client:Jedis,orderInfo:OrderInfo) :Unit ={
      val key = "order_info" + orderInfo.id
    saveToRedis(client,key,orderInfo)
  }

  def cacheOrderDetail(client:Jedis,orderDetail: OrderDetail):Unit ={
    val key = s"order_detail:${orderDetail.order_id}:${orderDetail.id}"
    saveToRedis(client,key,orderDetail)
  }
  import scala.collection.JavaConversions._

 def fullJoin(orderInfoStream:DStream[(String,OrderInfo)], orderDetailStream:DStream[(String,OrderDetail)]):DStream[SaleDetail] ={

   orderInfoStream.fullOuterJoin(orderDetailStream).mapPartitions(
     (it: Iterator[(String, (Option[OrderInfo], Option[OrderDetail]))]) =>{
           val client = RedisUtil.getClient
           val result = it.flatMap{

             case (orderId,(Some(orderInfo),Some(orderDetail))) =>
               println("some","some")
               cacheOrderInfo(client,orderInfo)
               val first = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
               val keys = client.keys(s"order_detail:${orderId}:*").toList
               first :: keys.map(key =>{
                 val orderDetailString = client.get(key)
                 client.del(key)

                 val orderDetail = JSON.parseObject(orderDetailString,classOf[OrderDetail])

                 SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
               })


             case (orderId,(Some(orderInfo),None)) =>
               println("some","None")
               cacheOrderInfo(client,orderInfo)
               val keys = client.keys(s"order_detial:${orderId}:*").toList

               keys.map(key =>{
                 val orderDetailString = client.get(key)
                 client.del(key)
                 val orderDetail = JSON.parseObject(orderDetailString,classOf[OrderDetail])
                 SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
               })

               case (orderId,(None,Some(orderDetail))) =>

             val orderInfoString = client.get("orer_info:" + orderId)
             println("None","Some")
             if(orderInfoString != null && orderInfoString.nonEmpty){
               val orderInfo = JSON.parseObject(orderInfoString,classOf[OrderInfo])
               SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
             }else{
               cacheOrderDetail(client,orderDetail)
               Nil
             }

           }
         client.close()
         result
   })
 }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")
    val ssc = new StreamingContext(conf, Seconds(3))
    // 1. 读取kafka中的两个topic, 得到两个流
    // 2. 对他们做封装  (join必须是kv形式的, k其实就是他们join的条件)
    val orderInfoStream: DStream[(String, OrderInfo)] = MykafkaUtil
      .getKafkaStream(ssc, Constant.TOPIC_ORDER_INFO)
      .map(s => {
        val orderInfo = JSON.parseObject(s, classOf[OrderInfo])
        (orderInfo.id, orderInfo)
      })
    val orderDetailStream: DStream[(String, OrderDetail)] = MykafkaUtil
      .getKafkaStream(ssc, Constant.TOPIC_ORDER_DETAIL)
      .map(s => {
        val orderDetail = JSON.parseObject(s, classOf[OrderDetail])
        (orderDetail.order_id, orderDetail) // order_id就是和order_info表进行管理的条件
      })
    // 3. 双流join
    val saleDetailStream: DStream[SaleDetail] = fullJoin(orderInfoStream, orderDetailStream)
    saleDetailStream.print(1000)

    ssc.start()
    ssc.awaitTermination()
  }
}
