package com.atguiug.day02

object bubbleSortDemo {
  def main(args: Array[String]): Unit = {
    val arr = Array(1,0,4,5)
    BubbleSort(arr)
    println(arr.toList)
  }

  def BubbleSort(array: Array[Int]) ={
    for(i <- 0 until array.length - 1){
      for(j <- 0 until  array.length - i - 1 ){
        if(array(j) > array(j+1)){
          Swap(array, j,j+1)
        }
      }

    }
  }

 def Swap(array: Array[Int], a: Int,b:Int) = {
    var temp = array(a)
    array(a) = array(b)
    array(b) = temp
  }
}
