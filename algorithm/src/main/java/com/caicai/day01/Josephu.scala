package com.atguiug.day01

object Josephu {
  def main(args: Array[String]): Unit = {
 print("\n留下来是：" + start(6,1,3))
  }

  def start(totalNumber:Int,startNumber:Int,IntervalNumber:Int): Int ={
    val circularLinkedList = new CircularLinkedList[Int]
    //先将几个人加入约瑟夫环中
    for(no<- 1 to totalNumber ){
      circularLinkedList.add(no)
    }
    var startNode = circularLinkedList.find(startNumber).pre
    while(circularLinkedList.length != 1){

      for(num <- 1 to IntervalNumber){
        startNode= startNode.next
      }
      circularLinkedList.delete(startNode.value)
      print(startNode.value + "->")
      startNode = startNode.pre
    }
    circularLinkedList.head.value
  }
}
