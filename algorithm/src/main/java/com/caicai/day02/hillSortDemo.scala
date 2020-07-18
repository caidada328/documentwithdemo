package com.atguiug.day02

object hillSortDemo {
  def main(args: Array[String]): Unit = {
    var array = Array(1,4,0,3)
    shellsort(array)
    println(array.toList)
  }

  def shellsort(array:Array[Int])={
    var gap = array.length / 2
    while(gap > 0){
      for(i <- gap until array.length ){
        for(j<- i-gap until(array.length - gap,gap)){
          if(array(j) < array(j + gap)){
            var temp = array(j)
            array(j) = array(j+gap)
            array(j+gap) = temp

          }
        }
      }
      gap /= 2
    }


  }

}
