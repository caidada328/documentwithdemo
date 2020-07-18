package com.atguiug.day01

object DoublyLinkedListDemo {
  def main(args: Array[String]): Unit = {

  }
}

class DoubleLinkedList[T]{
var head:Node = _
  var tail:Node = _

  def add(ele:T)={
    val newNode = Node(ele,null,null)
    if(head == null){
      head = newNode
      tail = head
    }else{
      tail.next = newNode
      newNode.pre = tail
      tail = newNode
    }
 true

  }

  def delete(ele:T):Boolean ={
    val targetNode:Node = find(ele)
    if(targetNode == null) {
       false
    }else{

      val preNode:Node = targetNode.pre
      val nextNode:Node = targetNode.next
      if(targetNode == head){
        nextNode.pre = null
        head = nextNode
      }else if(targetNode == tail){
        preNode.next = null
        tail = preNode
      }else{
        preNode.next = nextNode
        nextNode.pre = preNode
      }
      true
    }
  }



  def find(ele:T):Node={
    if(head == null) return null
    var tmp :Node = head
    while(tmp != null){
      if(tmp ==  ele) return tmp
      tmp = tmp.next
    }
     null
  }
  def printAll:Any={
    if(head == null) return
    var temp:Node = head
    while(temp!= null){
      print(temp + "->")
     temp = temp.next
    }
    println()
  }

  case class Node(value:T,var pre:Node,var next:Node)
}
