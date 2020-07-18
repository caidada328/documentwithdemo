
object DoublyLinkedListDemo {
    def main(args: Array[String]): Unit = {
        val list = new DoublyLinkedList[Int]
        list.add(10)
        list.add(20)
        list.printAll
        
        println("---------")
        list.delete(10)
        list.printAll
    }
}

//双向链表
class DoublyLinkedList[T] {
    
    case class Node(value: T, var pre: Node, var next: Node) {
        override def toString: String = value + ""
    }
    
    
    var head: Node = _
    var tail: Node = _
    
    def add(ele: T): Unit = {
        val newNode = Node(ele, null, null)
        if (head == null) {
            head = newNode
            tail = newNode
        } else {
            // 1. tail的next指向新节点
            tail.next = newNode
            //2. 新节点pre指向tail
            newNode.pre = tail
            //3. 让tail指向新节点
            tail = newNode
        }
    }
    
    // 删除指定的元素
    def delete(ele: T): Boolean = {
        // 1. 找到要删除的元素
        val targetNode: Node = find(ele)
        if (targetNode == null) false
        else {
            // 待删除元素的上一个节点
            val pre = targetNode.pre
            // 待删除元素的下一个节点
            val next = targetNode.next
            if (head == tail) { // 只有一个节点, 删除的时候就比较简单
                head = null
                tail = null
                
            } else if (targetNode.eq(head)) { // 如果删除的是head  eq判断地址值是否相等
                next.pre = null
                head = next
            } else if (targetNode.eq(tail)) { // 如果删除的是tail
                pre.next = null
                tail = pre
            } else {
                pre.next = next
                next.pre = pre
            }
            true
        }
    }
    
    def find(ele: T): Node = {
        var temp = head
        while (temp != null) {
            if (temp.value == ele) {
                return temp
            }
            temp = temp.next
        }
        null
    }
    
    
    // 打印链表中的元素
    def printAll = {
        if (head != null) {
            var temp = head
            do {
                print(temp.value + "->")
                temp = temp.next
            } while (temp != null)
            println()
        }
    }
}