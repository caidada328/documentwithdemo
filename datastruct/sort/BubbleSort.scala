
object BubbleSort {
    def main(args: Array[String]): Unit = {
        val arr1 = Array(30, 50, 70, 60, 10, 20)
        println(arr1.mkString(", "))
        println("----------")
        sort(arr1)
        println(arr1.mkString(", "))
    }
    
    // 在数组的原地排序
    def sort(arr: Array[Int]) = {
        // 外层表示需要对多少个元素进行排序.
        for(j <- 0 until arr.length - 1){
            // 排好最大的, 需要比较的次数是长度 - 1
            for (i <- 0 until arr.length - 1 - j) {
                if(arr(i) > arr(i + 1)){
                    swap(arr, i, i + 1)
                }
            }
        }
        
    }
    
    
}

/*
冒泡
    原理:
    
插入
选择

 */