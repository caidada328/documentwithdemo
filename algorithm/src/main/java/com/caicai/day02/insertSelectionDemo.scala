package com.atguiug.day02



object insertSelectionDemo {
  def main(args: Array[String]): Unit = {
    val arr = Array(1,0,4,5)
    insertSelection(arr)
    println(arr.toList)
  }

  def insertSelection(array: Array[Int]) = {
    for(i <- 0 until array.length - 1){
      for(j <- i+1 until(0,-1)){ //until(start,step) 起始位置与步长，步长可以是负数
        if(array(j)<array(j-1)){
          var temp = array(j)
          array(j)  = array(j - 1)
          array(j - 1) = temp
        }

      }
    }
  }
}
