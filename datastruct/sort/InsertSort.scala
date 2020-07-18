
object InsertSort {
    def main(args: Array[String]): Unit = {
        val arr1 = Array(30, 50, 70, 60, 10, 20)
        println(arr1.mkString(", "))
        println("----------")
        insertSort(arr1)
        println(arr1.mkString(", "))
    }
    
    def insertSort(arr: Array[Int]) = {
        for (i <- 0 until arr.length - 1) {
            breakable {
                for (j <- i + 1 until(0, -1)) {
                    // 后面小于前面的时候才交换
                    if (arr(j) < arr(j - 1)) {
                        swap(arr, j, j - 1)
                    } else break // 如果一旦不需要交换,
                }
            }
        }
    }
}

/*
插入排序:
 
 */