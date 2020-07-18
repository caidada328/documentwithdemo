
object MergeSort {
    
    def main(args: Array[String]) {
        val arr1: Array[Int] = randomArr
        println("----------")
        val time: Long = System.currentTimeMillis()
        
//        mergeSort(arr1)
       val arr2 =  arr1.sorted
        println(System.currentTimeMillis() - time)
        println(arr2.take(1000).mkString(", "))
    }
    
    
    // 对数组原地排序
    def mergeSort(arr: Array[Int]) {
        // 治(合并)  带哨兵的"哨兵"
        def merge1(arr: Array[Int], start: Int, mid: Int, stop: Int): Unit = {
            // 截取已经有序的左边数组 [start, mid]
            val left = arr.slice(start, mid + 1) :+ Int.MaxValue
            // // 截取已经有序的左边数组 [mid + 1, stop]
            val right = arr.slice(mid + 1, stop + 1) :+ Int.MaxValue
            
            var leftIndex = 0 // left 数组的索引
            var rightIndex = 0 // right 数组的索引
            for (i <- start to stop) { // i表示原数组中的索引
                if (left(leftIndex) <= right(rightIndex)) {
                    arr(i) = left(leftIndex)
                    leftIndex += 1
                } else {
                    arr(i) = right(rightIndex)
                    rightIndex += 1
                }
            }
        }
        
        // 治(合并)
        def merge(arr: Array[Int], start: Int, mid: Int, stop: Int): Unit = {
            // 截取已经有序的左边数组 [start, mid]
            val left = arr.slice(start, mid + 1)
            // // 截取已经有序的左边数组 [mid + 1, stop]
            val right = arr.slice(mid + 1, stop + 1)
            
            var leftIndex = 0 // left 数组的索引
            var rightIndex = 0 // right 数组的索引
            for (i <- start to stop) { // i表示原数组中的索引
                if (leftIndex == left.length) { // 左边已经取完
                    arr(i) = right(rightIndex)
                    rightIndex += 1
                } else if (rightIndex == right.length) {
                    arr(i) = left(leftIndex)
                    leftIndex += 1
                } else if (left(leftIndex) <= right(rightIndex)) {
                    arr(i) = left(leftIndex)
                    leftIndex += 1
                } else {
                    arr(i) = right(rightIndex)
                    rightIndex += 1
                }
            }
        }
        
        // [start, stop]
        def sort(arr: Array[Int], start: Int, stop: Int): Unit = {
            if (start >= stop) return
            
            // 分
            val mid = (start + stop) / 2
            // 对左边排序
            sort(arr, start, mid)
            // 对右边排序
            sort(arr, mid + 1, stop)
            // 治 (合并)
            merge1(arr, start, mid, stop)
            
        }
        
        sort(arr, 0, arr.length - 1)
    }
}

/*
归并排序:
    典型的分治思想
 */