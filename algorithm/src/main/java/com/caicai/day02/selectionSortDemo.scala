package com.atguiug.day02


import com.atguiug.day02._

object selectionSortDemo {
  def main(args: Array[String]): Unit = {
    val arr = Array(1,0,4,5)
    selectionSort(arr)
    println(arr.toList)
  }

  def selectionSort(array: Array[Int])={
    var minValue = array(0)
    var count  = 0
    for(i <- 0 until array.length - 1){
      for(j <- count until array.length - 1){
        if(array(i) > array(j)){
         var temp = array(j)
           array(j) = array(i)
          array(i) = temp
        }
        count += 1

      }
    }
  }
}
