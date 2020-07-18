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

/**
 * Author caicai
 * Date 2020/4/7 14:33
 */
object SaleDetailApp {
    /**
     * 写数据到redis
     *
     * @param client
     * @param key
     * @param value
     */
    def saveToRedis(client: Jedis, key: String, value: AnyRef): Unit = {
        import org.json4s.DefaultFormats
        val json = Serialization.write(value)(DefaultFormats)
        // 把数据存入到redis
//        client.set(key, json)
        // 添加了过期时间  超过60*30秒之后这个key会自动删除
        client.setex(key, 60 * 30 , json)
    }
    
    /**
     * 缓存OrderInfo
     *
     * @param orderInfo
     * @return
     */
    def cacheOrderInfo(client: Jedis, orderInfo: OrderInfo) = {
        val key = "order_info:" + orderInfo.id
        saveToRedis(client, key, orderInfo)
    }
    
    /**
     * 把orderDetail缓存到Redis中
     *
     * @param client
     * @param orderDetail
     * @return
     */
    def cacheOrderDetail(client: Jedis, orderDetail: OrderDetail) = {
        val key = s"order_detail:${orderDetail.order_id}:${orderDetail.id}"
        saveToRedis(client, key, orderDetail)
    }
    
    
    import scala.collection.JavaConversions._ //把java集合使用scala的方法

    
    /**
     * 对传入的两个流进行fullJoin
     *
     * @param orderInfoStream
     * @param orderDetailStream
     * @return
     */
    def fullJoin(orderInfoStream: DStream[(String, OrderInfo)],
                 orderDetailStream: DStream[(String, OrderDetail)]): DStream[SaleDetail] = {
        orderInfoStream.fullOuterJoin(orderDetailStream).mapPartitions(it => {
            // 1. 获取redis客户端
            val client: Jedis = RedisUtil.getClient
            
            // 2. 对各种延迟做处理  (如果返回一个就把一个放在集合中, 如果返回的是空, 就返回一个空集合 ...)
            val result = it.flatMap {
                // a: orderInfo和orderDetail都存在
                case (orderId, (Some(orderInfo), Some(orderDetail))) =>
                    println("some", "some")
                    // 1. 写到缓冲区(向redis写数据)
                    cacheOrderInfo(client, orderInfo)
                    // 2. 把orderInfo和oderDetail的数据封装到一起, 封装到样例类中
                    val first = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                    // 3. 去缓冲区找已经进入缓冲区的OrderDetail
                    val keys: List[String] = client.keys(s"order_detail:${orderId}:*").toList
                    // 3.1 集合中会有多个OrderDetail
                    first :: keys.map(key => {
                        val orderDetailString: String = client.get(key)
                        client.del(key)  // 防止这个orderDetail被重复join

                        //与json4s的作用相反
                        val orderDetail = JSON.parseObject(orderDetailString, classOf[OrderDetail])
                        
                        SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                        
                    })
                
                // c: OrderInfo存在, OrderDetail没有对应的的数据
                case (orderId, (Some(orderInfo), None)) =>
                    println("some", "None")
                    // 1. orderInfo要写入缓存 (考虑对应的OrderDetail有多个, 可能还在延迟中)
                    cacheOrderInfo(client, orderInfo)
                    // 2. 根据orderId去缓存中读取对应的 多个OrderDetail的信息(集合)
                    // Set(order_detail:1:1, ....)
                    val keys: List[String] = client.keys(s"order_detail:${orderId}:*").toList
                    
                    // 3. 集合中会有多个OrderDetail
                    keys.map(key => {
                        val orderDetailString: String = client.get(key)
                        client.del(key)  // 防止这个orderDetail被重复join
                        val orderDetail = JSON.parseObject(orderDetailString, classOf[OrderDetail])
                        SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                    })
                
                // b: oderInfo没有对应的数据, orderDetail存在
                case (orderId, (None, Some(orderDetail))) =>
                    // 1. 根据orderDetail中的orderId去缓存读取对应的orderInfo信息
                    val orderInfoString: String = client.get("order_info:" + orderId)
                    println("None", "some")
                    // 2. 读取之后, 有可能读到对应的OderInfo信息, 也有可能没有读到. 分别处理
                    // 2.1 读到, 把数据封装SaleDetail中去
                    if (orderInfoString != null && orderInfoString.nonEmpty) {
                        val orderInfo: OrderInfo = JSON.parseObject(orderInfoString, classOf[OrderInfo])
                        SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
                    } else { // 2.2 读不到, 把OrderDetail写到缓存
                        cacheOrderDetail(client, orderDetail)
                        Nil
                    }
                
            }
            // 3. 关闭客户端
            client.close()
            
            // 4. 返回处理后的结果
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
        // 4. 根据用户的id反查mysql中的user_info表, 得到用户的生日和性别
        
        
        // 5. 把详情写到es中
        
        
        ssc.start()
        ssc.awaitTermination()
    }
}

/*
redis
redis写到什么样的数据类型

hash?
    key                              value(hash)
    "order_info"                     field                              value
                                     order_id                           整个order_info的所有数据(json字符串)
    
    "order_detail"                   order_id:order_detail_id           整个order_detail的所有数据(json字符串)
    
-----

String(json) ?
    
    key                                                     value(字符串)
    "order_info:" + order_id                                整个order_info的所有数据(json字符串)
    
    "order_detail:" + order_id + ":" + order_detail_id      整个order_detail的所有数据(json字符串)
    




 */
/*
object SaleDetailApp2{

   /* def  saveToRedis (client: Jedis, key: String, value: AnyRef):Unit={

        import org.json4s.DefaultFormats
        val json = Serialization.write(value)(DefaultFormats)

        client.setex(key,60*30,json)
    }*/

    import scala.collection.JavaConversions._

   private def saveToRedis(client: Jedis, key: String, value: AnyRef) = {
       import org.json4s.DefaultFormats

       val json = Serialization.write(value)(DefaultFormats)

       client.setex(key, 60 * 30, json)
   }

    def cacheOederInfo(client:Jedis,orderInfo:OrderInfo) ={
        val key = "order_info" + orderInfo.id
        saveToRedis(client,key,orderInfo)
    }

    def cacheOrderDetail(client: Jedis, orderDetail: OrderDetail)={
        val key =  s"order_detail:${orderDetail.order_id}:${orderDetail.id}"
        saveToRedis(client,key,orderDetail)

    }


    def fullJoin(orderInfoStream:DStream[(String,OrderInfo)],
                 orderDetailStream:DStream[(String,OrderDetail)]):DStream[SaleDetail] = {

        orderInfoStream.fullOuterJoin(orderDetailStream).mapPartitions(it =>{


            val client:Jedis = RedisUtil.getClient

            val result  = it.flatMap{

                case (orderId,(Some(orderInfo: OrderInfo),Some(orderDetail: OrderDetail))) =>
                cacheOederInfo(client,orderInfo)
                val first = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)

                val keys :List[String] = client.keys(s"order_detail:${orderId}:*").toList

                first :: keys.map(key => {
                    val orderDetailString = client.get(key)
                    client.del(key)
                    val orderDetail = JSON.parseObject(orderDetailString,classOf[OrderDetail])
                    SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                })

                case(orderId,(Some(orderInfo),None)) =>

                cacheOederInfo(client,orderInfo)
                val keys = client.keys(s"order_detail:${orderId}:*").toList

                keys.map(key =>{
                    val orderDetailString = client.get(key)
                    client.del(key)
                    val orderDetail = JSON.parseObject(orderDetailString,classOf[OrderInfo])

                    SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                })


                case (orderId, (None, Some(orderDetail)) =>
                val orderINfoString = client.get("order_info" + orderId)

                if(orderINfoString != null && orderINfoString.nonEmpty){

                    val orderInfo = JSON.parseObject(orderINfoString,classOf[OrderInfo])
                    SaleDetail().mergeOrderDetail(orderDetail).mergeOrderInfo(orderInfo)::Nil
                }else{
                    cacheOrderDetail(client,orderDetail)
                    Nil
                }



            }
        }


    )

        def main(args: Array[String]): Unit = {
            val conf = new SparkConf().setAppName("SaleDetailApp").setMaster("liocal[*]")
            val ssc = new StreamingContext(conf,Seconds(6))

            val orderInfoStream = MykafkaUtil.getKafkaStream(
                ssc,
                Constant.TOPIC_ORDER_INFO
            ).maps(s => {
                val orderInfo = JSON.parseObject(s,classOf[OrderInfo])
                (orderInfo.id,orderInfo)
            })

            val orderDetailSream = MykafkaUtil.getKafkaStream(
                ssc,
                Constant.TOPIC_ORDER_DETAIL

            ).map(s=>{
                val orderDetail = JSON.parseObject(s,classOf[OrderDetail])
                (orderDetail.order_id,orderInfo)
                val saleDetailStream = fullJoin(orderInfoStream, orderDetailStream)
                ssc.start()
                ssc.awaitTermination()
            })
        }

    }

}*/
