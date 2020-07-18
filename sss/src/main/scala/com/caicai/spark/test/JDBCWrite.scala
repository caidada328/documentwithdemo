/*
package com.caicai.spark.test

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}


object JDBCWrite {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCRead").setMaster("local[*]")
    val sc =new SparkContext(conf)
    val sourceRDD = sc.parallelize(1 to 100).map(x =>(x,"zhiing"+ x))
    val url = "jdbc:mysql://loaclhost:3306/test"
    val password = ""
    val user = "root"
    val sql = "insert into user values(?,?)"
    sourceRDD.foreachPartition(
      it =>{
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection(url,user,password)
        val pstmt = conn.prepareStatement(sql)
        var count = 0
        it.foreach{
          case (age,name) =>
            pstmt.setInt(1,age)
            pstmt.setString(2,name)
            pstmt.addBatch()
            count += 1
            if(count %100 == 0){
              pstmt.executeBatch()
              Thread.sleep(1000)
            }
        }

        pstmt.executeBatch()
        pstmt.close()
        conn.close()
      }
    )
    sc.stop()
  }
}
*/
