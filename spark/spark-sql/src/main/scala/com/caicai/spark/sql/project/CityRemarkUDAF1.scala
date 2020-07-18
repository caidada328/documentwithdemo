package com.caicai.spark.sql.project
import java.text.DecimalFormat

import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


object CityRemarkUDAF1 extends UserDefinedAggregateFunction {
    override def inputSchema:StructType = StructType(Array(StructField("city", StringType)))
    override def bufferSchema:StructType =StructType(Array(StructField("map", MapType(StringType,LongType)),StructField("total",LongType)))

    override def dataType: DataType = StringType
    override def deterministic: Boolean = true
    override def initialize(buffer:MutableAggregationBuffer): Unit = {
      buffer(0) = Map[String,Long]()
      buffer(1) = 0L
    }
    override def update(buffer:MutableAggregationBuffer, input: Row): Unit = {
      input match{
        case Row(cityname:String) =>
          buffer(1) = buffer.getLong(1) + 1L
          val map: collection.Map[String, Long] = buffer.getMap[String,Long](0)
         buffer(0) =  map +(cityname -> (map.getOrElse(cityname,0L) + 1L))

        case _ =>
      }
    }
    override def merge(buffer1:MutableAggregationBuffer, buffer2:Row): Unit = {
      buffer1(0) = buffer1.getLong(1) +  buffer2.getLong(1)

      val map1: collection.Map[String, Long] = buffer1.getMap[String,Long](0)
      val map2: collection.Map[String, Long] = buffer2.getMap[String,Long](0)

      map1.foldLeft(map2){
        case(map,(cityname,count)) =>
          map + (cityname -> (map.getOrElse(cityname,0L) + count))
      }


    }
    override def evaluate(buffer:Row): Any = {
      val cityandcount = buffer.getMap[String,Long](0)
      val total = buffer.getLong(1)

      val cityandcountTop2 = cityandcount.toList.sortBy(-_._2).take(2)
       var cityremark = cityandcountTop2.map{
         case (cityName,count1)=> Result(cityName,count1.toDouble/total*100)
       }
      cityremark :+= Result("其它",cityremark.foldLeft(100D)(_-_.Per))
      val result = new DecimalFormat(".00").format(cityremark)
      println(result)
    }

  }

case class Result(cityName:String,Per:Double)