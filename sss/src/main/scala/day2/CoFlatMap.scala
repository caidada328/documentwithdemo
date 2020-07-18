package day2

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoFlatMap {
   def main(args: Array[String]): Unit = {
     val env = StreamExecutionEnvironment.getExecutionEnvironment
     env.setParallelism(1)
     //将key相同的两个流connect在一块儿在keyby在flatmap(使用自定义map输出)一下输出
     val one = env.fromElements((1,3))
     val two = env.fromElements((1,"two"))

     val connected  = one.keyBy(_._1).connect(two.keyBy(_._1))

     connected.flatMap(new MyCoFlatMap).print()

     env.execute()
  }
  class MyCoFlatMap extends CoFlatMapFunction[(Int,Int),(Int,String),String] {
    override def flatMap1(value: (Int, Int), out: Collector[String]): Unit = out.collect(value._2.toString + "来自第一条流")
    override def flatMap2(value: (Int, String), out: Collector[String]): Unit =out.collect(value._2.toString + "来自第二条流")
      ???
  }

}
