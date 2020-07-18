/*
package com.caicai.spark.test

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}


object JDBCRead {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DBCRead").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val user = "root"
    val password = "aaaaaa"
    val rdd = new JdbcRDD[String](
      sc,
      () =>{
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection(url,user,password)
      },
      "select * from user where id > ? and id < ?",
      1,
      10,
      2,
      (resultSet: ResultSet) => resultSet.getString(2)

    )
    rdd.collect().foreach(println)
    sc.stop()
  }
}
*/
