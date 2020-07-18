package com.atguiug.day02

object binarySearchDemo {
  def main(args: Array[String]): Unit = {
  var array = Array(1,4,0,3)
    println(binarySearch(array, 0, 3, 4))
    println(binarySearch(array, 0, 3, 5))
  }

  def binarySearch(array: Array[Int],low:Int,high:Int,ele:Int):Int= {
   if(low > high) -1
    else
    {
      val mid = (low + high) / 2
      val midValue = array(mid)
        if (ele == midValue) mid
        else if (ele > midValue) {
          binarySearch(array, mid + 1, high, ele)
        } else {
          binarySearch(array, low, mid - 1, ele)
        }
      }
  }
}
