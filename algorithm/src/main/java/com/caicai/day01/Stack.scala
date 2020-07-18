package com.atguiug.day01

import scala.collection.mutable
import scala.reflect.ClassTag

/*1.栈和队列只是进去的元素合出来的元素不一致，栈只操作栈顶的元素，所以说的是不需要定义栈底
*
* */
object Stack {
  def main(args: Array[String]): Unit = {
    val stack = new ArrayStack[Int](5)
    stack.push(3)
    stack.push(4)
    stack.push(5)

    println(stack.pop)
    println(stack.getPeak)
  }

  class ArrayStack[T:ClassTag](val maxsize:Int){
    private val arr = new Array[T](maxsize)

    private var top = -1

    def isEmpty = top == -1
    def isFull = top ==maxsize -1

    def push(ele:T):Any={
      if(isFull)throw new UnsupportedClassVersionError("栈已经满了，无法添加新的元素")

      top += 1
      arr(top) =ele


    }

    def pop() ={
      if(isEmpty) None
      else{
      var  res = arr(top)

      top -= 1
      Some(res)
      }

    }
    def getPeak={
    if(isEmpty) None
    else Some(arr(top))

    }
  }


}
