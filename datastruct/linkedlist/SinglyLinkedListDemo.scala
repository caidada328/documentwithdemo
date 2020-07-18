
object SinglyLinkedListDemo {
    def main(args: Array[String]): Unit = {
        val list = new SinglyLInkedList[Int]
        list.add(1)
        list.add(10)
        list.add(100)
        list.add(1000)
        
        list.printAll
    
        println(list.contain(100))
        println(list.contain(200))
    }
}


class SinglyLInkedList[T] {
    
    // 用来表示单向链表中的节点
    case class Node(vale: T, var next: Node)
    
    // 记录链表的头节点 必须
    var head: Node = _
    // 为了方便添加元素, 可选
    var tail: Node = _
    
    // 向链表中添加元素
    def add(ele: T): Unit = {
        // 1. 把要添加值, 封装到一个Node中
        val newNode = Node(ele, null)
        
        // 表示第一次添加
        if (head == null) {
            head = newNode
            tail = newNode
        } else {
            // 2. 让tail的next指针指向新节点
            tail.next = newNode
            // 3. 再让tail指向最后一个元素(新节点)
            tail = newNode
        }
    }
    // 判断链表中元素是否存在
    def contain(ele: T) : Boolean = {
        if(head == null) return false
        var temp = head
        do {
            if(temp.vale == ele) return true
            temp = temp.next
        } while (temp != null)
        false
    }
    
    // 打印链表中的元素
    def printAll = {
        if (head != null) {
            var temp = head
            do {
                println(temp.vale)
                temp = temp.next
            } while (temp != null)
        }
    }
    
}