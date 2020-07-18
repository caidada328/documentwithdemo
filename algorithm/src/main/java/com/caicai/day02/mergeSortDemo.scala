package com.atguiug.day02

object mergeSortDemo {
  def main(args: Array[String]): Unit = {
    val arr = Array(1,4,2,3,9)
    mergeSort(arr,0,4)
    println(arr.toList)
  }

  def merge(array: Array[Int], low: Int, mid: Int, high: Int): Unit = {
    val left = array.slice(low,mid+1):+ Int.MaxValue
    val right= array.slice(mid+1,high):+Int.MaxValue
    var leftIndex = 0
    var rightIndex = 0
    for(i <- low until high-1){
      if(left(leftIndex) < right(rightIndex)){
        array(i) = left(leftIndex)
        leftIndex += 1
      }else{
        array(i) = right(rightIndex)
        rightIndex += 1
      }
    }
  }

  def mergeSort(array: Array[Int], low: Int, high: Int):Unit={
  if(low >= high) return //注意必须是大于等于号，因为等于的时候已经不行了
  val mid = (low + high )/2
  mergeSort(array,low,mid)
  mergeSort(array,mid+1,high)
  merge(array,low,mid,high)
}

}
