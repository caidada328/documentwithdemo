package com.caicai.spark1015.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author caicai
 * Date 2020/3/11 16:31
 */
/*
object Hello {
    def main(args: Array[String]): Unit = {
        // 1. 创建一个SparkContext  打包的时候, 把master的设置去掉, 在提交的时候使用 --maser 来设置master
        val conf: SparkConf = new SparkConf().setAppName("Hello").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        // 2. 从数据源得到一个RDD
        val lineRDD: RDD[String] = sc.textFile("C:\\Scalaa\\spark1015-master\\spark-core\\src\\main\\scala\\com\\caicai\\spark1015\\day01\\aa.txt")
        // 3. 对RDD做各种转换
        val resultRDD: RDD[(String, Int)] = lineRDD.flatMap(_.split("\\W"))
            .map((_, 1))
            .reduceByKey(_ + _)
        
        // 4. 执行一个行动算子   (collect: 把各个节点计算后的数据, 拉取到驱动端)
        val wordCountArr = resultRDD.collect()
        wordCountArr.foreach(println)
        // 5. 关闭SparkContext
        sc.stop()
    }
}
*/
object Hello{
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("test")
        val sparkcontext = new SparkContext(conf)
        val inputStream = sparkcontext.textFile("C:\\Scalaa\\spark1015-master\\spark-core\\src\\main\\scala\\com\\caicai\\spark1015\\day01\\aa.txt")
          .flatMap(a => a.split("\\W+")).map((_,1)).reduceByKey(_+_).collect()
        inputStream.foreach(println)
        sparkcontext.stop()
    }
}