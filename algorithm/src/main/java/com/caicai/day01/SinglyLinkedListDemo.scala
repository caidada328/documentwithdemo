package com.atguiug.day01

object SinglyLinkedListDemo {

  def main(args: Array[String]): Unit = {

  }
}
  class SinglyLinkedList[T]{
    var tail:Node = _
    var head:Node = _
    def add(ele:T) ={
      if(head == null ){
        head = Node(ele,null)
        tail = head
      }else{
         tail.next = Node(ele,null)
        tail = tail.next
      }

    }

    def delete(ele:T):Boolean={
      if(head == null )return false
      if (head != null && head == ele){
        if(head.eq(tail)){
          head = null//当只有一个元素的时候，那么就这样删除为什么呢？
        }
        head = head.next
        return true
      }else{
        var currentNode  = head
        var nextNode = currentNode.next
        while(nextNode != null){

          if(nextNode == ele){
            currentNode.next = nextNode.next
            if(nextNode.eq(tail)){
              tail = currentNode //将nextNode之前的currentNode赋值给tail，重新定位tail
            }
            return true
          }
          currentNode = nextNode
          nextNode = nextNode.next
        }
      }
     false
    }

    def contains(ele:T):Boolean={
      if(head == null ) return false

      var tempNode = head
      while(tempNode.next != null){
        if(tempNode == ele)
          return true
        tempNode = tempNode.next
      }
    false

    }
    def printInfo ={
      if(head == null)  throw new UnsupportedOperationException("该链表元素为空")
      var temp = head
      while(temp.next != null){
        print(temp.value + " ->")
        temp = temp.next
      }
    }


    case class Node(value:T,var next:Node)

    }



