package com.caicai.gmall.realtime.app
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.caicai.gmall.common.{Constant, ESUtil}
import com.caicai.gmall.realtime.app.gSaleDetailApp1.{cacheOrderDetail, joinUser}
import com.caicai.gmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.caicai.gmall.realtime.util.{MykafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

object SaleDetailApp111 {


  def saveToRedis(client:Jedis,key:String,value:AnyRef) = {
    import org.json4s.DefaultFormats
    val json = Serialization.write(value)(DefaultFormats)
    client.setex(key,60 *30,json)
  }

  def cacheOrderInfo(client:Jedis,orderInfo:OrderInfo) ={
    val key = "order_info" + orderInfo.id
    saveToRedis(client,key,orderInfo)
  }


  def cacheOrderDetail(client:Jedis,orderDetail: OrderDetail) ={
    val key = s"order_detail:${orderDetail.order_id}:${orderDetail.id}"
    saveToRedis(client,key,orderDetail)
  }

  import scala.collection.JavaConversions._

  def fullJoin(orderInfoStream:DStream[(String,OrderInfo)],orderDetailStream:DStream[(String,OrderDetail)]):DStream[SaleDetail] ={
    orderInfoStream.fullOuterJoin(orderDetailStream).mapPartitions(it =>{
      val client = RedisUtil.getClient
      val result: Iterator[SaleDetail] = it.flatMap{
        case (orderId, (Some(orderInfo),opt)) =>
          cacheOrderInfo(client,orderInfo)
          val keys = client.keys(s"order_detail:${orderId}").toList
          keys.map( (key: String) => {
            val  orderDetailString = client.get(key)
            client.del(key)
            val orderDetail = JSON.parseObject(orderDetailString,classOf[OrderDetail])
            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
          })  ::: (opt match {
            case Some(orderDetail) =>
              SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
            case None =>
              Nil
          })
        case (orderId,(None,Some(orderDetail))) =>
        val orderInfoString = client.get("order_info" +orderId)
        println("None","Some")
        if(orderInfoString != null && orderInfoString.nonEmpty){
          val orderInfo = JSON.parseObject(orderInfoString,classOf[OrderInfo])
          SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
        } else { // 2.2 读不到, 把OrderDetail写到缓存
          cacheOrderDetail(client, orderDetail)
          Nil
        }


      }
        client.close()
      result
    })
  }

  def joinUser(saleDetailStream:DStream[SaleDetail],ssc: StreamingContext)= {
    val url = "jdbc:mysql://hadoop104:3306/gmall1105"
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123321")
    val spark  =SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()
    import spark.implicits._
    saleDetailStream.transform(SaleDetailRDD =>{
      val userInfoRDD = spark.read.jdbc(url,"user_info",props).as[UserInfo].rdd.map(user =>(user.id,user))
      SaleDetailRDD.map(saleDetail =>(saleDetail.user_id,saleDetail)).join(userInfoRDD)
        .map {
          case (_, (saleDetail, userInfo)) =>
            saleDetail.mergeUserInfo(userInfo)

        }
    })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")
    val ssc = new StreamingContext(conf,Seconds(3))

    val orderInfoStream = MykafkaUtil.getKafkaStream(ssc,Constant.TOPIC_ORDER_INFO)
      .map(s =>{
        val orderInfo = JSON.parseObject(s,classOf[OrderInfo])
        (orderInfo.id,orderInfo)
      })
    val orderDetailStream = MykafkaUtil.getKafkaStream(ssc,Constant.TOPIC_ORDER_INFO)
      .map(s =>{
        val orderDetail = JSON.parseObject(s,classOf[OrderDetail])
        (orderDetail.order_id,orderDetail)
      })

    var saleDetailStream = fullJoin(orderInfoStream, orderDetailStream)

    saleDetailStream = joinUser(saleDetailStream,ssc)

    saleDetailStream.foreachRDD(rdd =>{
      ESUtil.insertBulk(Constant.INDEX_SALE_DETAIL,rdd.collect().toIterator)
    })

    ssc.start()
    ssc.awaitTermination()


  }
}
