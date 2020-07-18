package com.atguiug.day01

import  com.atguiug.day01.DoubleLinkedList


object CircularLinkedListDemo {
  def main(args: Array[String]): Unit = {

  }
}


class CircularLinkedList[T] extends  DoubleLinkedList[T]{

  //定义循环链表的元素的个数
  private var _len = 0
  //返回循环链表的个数，因为将这个length属性使用private保护起来了，所以需要定义一个方法来返回长度
  def length = _len

  override def add(ele: T): Boolean = {
    super.add(ele)

    head.pre = tail
    tail.next = head

    _len += 1
    true

  }

  override def delete(ele: T): Boolean = {
    if(super.delete(ele)){
      head.pre = tail
      tail.next = head
      _len -= 1
      true
    }else{
      false
    }
  }
  override def find(ele: T): Node = {
    var temp = head
    while(temp != null){
      if(temp.value == ele ) return temp
      if(temp.eq(tail)) return null
      temp = temp.next

    }
    null
  }

  override def printAll: Any = {
    if(head == null) return
    var temp = head
    while(true){
      if(temp.eq(tail)){
        println(temp.value)
      }else{
        print(temp.value + "->")
      }
      temp = temp.next
    }
  }
}