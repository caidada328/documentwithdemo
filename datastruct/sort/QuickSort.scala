
object QuickSort {
    
    def main(args: Array[String]): Unit = {
//        val arr1 = randomArr
        val arr1 = Array(30, 50, 70, 60, 10, 20)
        //        println(arr1.mkString(", "))
        println("----------")
        val time = System.currentTimeMillis()
        val sortedList = scalaQuickSort(arr1)
        //        BubbleSort.sort(arr1)
        println(System.currentTimeMillis() - time)
        println(sortedList.take(100).mkString(", "))
    }
    
    def scalaQuickSort2(list: List[Int]): List[Int] = {
        list match {
            case p :: rest =>
                scalaQuickSort2(rest.filter(_ <= p)) ::: p :: scalaQuickSort2(rest.filter(_ > p))
            case Nil => Nil
        }
    }
    
    // 不是原地排序, 而是返回排好序的新的数组. 原来数组不坐任何变化
    def scalaQuickSort(arr: Array[Int]): Array[Int] = {
        arr match {
            // Array(10)
            case Array(p, rest@_*) =>
                /*// 找到小于p的所有元素
                val left = scalaQuickSort(rest.filter(_ <= p).toArray)
                // 小于大于p的所有元素
                val right = scalaQuickSort(rest.filter(_ > p).toArray)*/
                val (left, right): (Seq[Int], Seq[Int]) = rest.partition(_ <= p)
                (scalaQuickSort(left.toArray) :+ p) ++scalaQuickSort(right.toArray)
            case Array() => Array[Int]()
        }
    }
    
    
    // 对数组原地排序
    def quickSort(arr: Array[Int]) = {
        
        def partition(arr: Array[Int], left: Int, right: Int): Int = {
            // 基准值
            val p = arr(left)
            // 左指针
            var low = left
            // 右指针
            var high = right
            
            while (low < high) { // 只要左指针还在右指针的左边, 就应该一直找下去
                // 左指针向右跑, 跑到大于基准值的是停下来
                while (low <= right && arr(low) <= p) { // 左指针不要超过右指针
                    low += 1
                }
                // 右指针向左跑, 跑到小于基准值的时候停下来
                while (high >= low && arr(high) > p) {
                    high -= 1
                }
                // 交换他们的元素
                if (low < high) {
                    swap(arr, low, high)
                }
            }
            // 返回基准值的正确位置
            swap(arr, left, high)
            high
        }
        
        // 排序, left开始的坐标, right 结束的坐标  [left, right]
        def quick(arr: Array[Int], left: Int, right: Int): Unit = {
            // 左指针与右指针已经重合, 到了右指针的右边, 则无需排序
            if (left >= right) return
            
            // 基准值的位置   10  保证位置10左边元素全部小于位置10元素, 右边全部大于
            val mid = partition(arr, left, right)
            // 对左边排序
            quick(arr, left, mid - 1)
            // 对右边排序
            quick(arr, mid + 1, right)
            
        }
        
        quick(arr, 0, arr.length - 1)
        
    }
}

/*
快速排序:
    数据量大的时候, 效果特别明显
    
    分治思想:
    


*/