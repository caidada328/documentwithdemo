package day2

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._
//将key相同的两个流connect在一块儿在keyby在map一下输出
object CoMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val one = env.fromElements((1,3))
    val two = env.fromElements((1,"two"))

    val connected  = one.keyBy(_._1).connect(two.keyBy(_._1))

    connected.map(new MyCoMap).print()

    env.execute()
  }

  class MyCoMap extends CoMapFunction[(Int,Int),(Int,String),String] {
    override def map1(value: (Int, Int)): String = value._2.toString + "\n from first stream"
    override def map2(value: (Int, String)): String = value._2.toString + "\n from second stream"
  }
}
