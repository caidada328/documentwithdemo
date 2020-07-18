package com.caicai.gmall.realtime.app
import com.alibaba.fastjson.JSON
import com.caicai.gmall.common.Constant
import com.caicai.gmall.realtime.bean.OrderInfo
import com.caicai.gmall.realtime.util.MykafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("OrderApp2")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sourceStream = MykafkaUtil.getKafkaStream(ssc,Constant.TOPIC_ORDER_INFO)


    import org.apache.phoenix.spark._
    val orderInfoStream = sourceStream.map(s => JSON.parseObject(s,classOf[OrderInfo]))
    orderInfoStream.foreachRDD(rdd =>{
      rdd.saveToPhoenix("GMALL_ORDER_INFO1015",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS",
          "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS",
          "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO","TRADE_BODY",
          "CREATE_DATE", "CREATE_HOUR"),
        zkUrl = Some("hadoop104,hadoop105,hadoop106:2181"))

      })
    ssc.start()
    ssc.awaitTermination()

}
}