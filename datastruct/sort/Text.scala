
object Text {
    def main(args: Array[String]): Unit = {
        /*val arr = (Array[Int]() :+ 10) ++ Array[Int]()
        println(arr.mkString(","))
        
        arr match {
            case Array(p, rest@_*) =>
                println(p)
                println(rest.size)
        }*/
        
        val arr1 = Array[Int]().toList
        val arr2 = arr1.partition(_ <= 30)
        println(arr2)
    }
}
