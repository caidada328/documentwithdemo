package com.atguiug.day01

import scala.reflect.ClassTag

object queue {

  def main(args: Array[String]): Unit = {
     val queue = new ArrayQueue[Int](5)

    queue.enqueue(10)
    queue.enqueue(90)

    queue.dequeue
    println(queue)


    queue.printQueue
  }
}
class ArrayQueue[T:ClassTag]( val initSize:Int) {
  //定义数组，用来存储数据
  val arr:Array[T] = new Array[T](initSize)

  var head = 0
  var tail = 0
  var count = 0
  def isEmpty = count==0
  def isFull = count == initSize

  def enqueue(ele:T)={
    if (isFull) throw new UnsupportedOperationException("队列已经满，无法添加元素")


      arr(tail) = ele

      tail += 1

      count += 1
    if(tail == initSize) tail = 0


  }

  def dequeue={

    if (isEmpty) None
    val result = arr(head)

    head += 1

    count -= 1

    result
    //将数组循环使用，队列其实就是数组的一部分，
    if(head == initSize) head = 0
    Some(result)
  }

  def printQueue:Any={
    for(i <- head until tail){
      print(arr(i) + "->")
    }
  }

}