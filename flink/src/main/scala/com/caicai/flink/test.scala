package com.atguiug.flink

object test {
  def main(args: Array[String]): Unit = {
    val source = List(("a s d e",1),("a s d",2))
    /*val r = source.map(kv=> ((kv._1+" ") * kv._2).trim).flatMap(_.split(" "))
        .groupBy(x => x)
        .mapValues(_.size)
        .toList
        .sortBy(-_._2)*/



      var r = source.flatMap(kv =>{
        kv._1.split(" ").map((_,kv._2))
      })  //List((a,1), (s,1), (d,1), (e,1), (a,2), (s,2), (d,2))
      .groupBy(_._1) //Map(e -> List((e,1)), s -> List((s,1), (s,2)), d -> List((d,1), (d,2)), a -> List((a,1), (a,2)))
          .mapValues(list =>{
           list.foldLeft(0)(_+_._2)
          }) //Map(e -> 1, s -> 3, d -> 3, a -> 3)
          .toList
          .sortBy(_._2)


    println(r)

}
}