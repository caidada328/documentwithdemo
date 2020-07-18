
object CircleDoublyLinkedListDemo {
    def main(args: Array[String]): Unit = {
        val list = new CircleDoublyLinkedList[Int]
        list.add(10)
                list.add(20)
                list.add(30)
                list.add(40)
        list.printAll
        println("-----------")
        println(list.delete(10))
        list.printAll
        println("-----------")
        list.add(50)
        list.printAll
    }
}

class CircleDoublyLinkedList[T] extends DoublyLinkedList[T] {
    override def add(ele: T): Unit = {
        super.add(ele)
        // 让head和tail形成一个环
        // 1. head的pre指向tail
        head.pre = tail
        // 2. tail的next指向head
        tail.next = head
    }
    
    override def printAll: Unit = {
        if (head != null) {
            var temp = head
            do {
                println(temp.value)
                temp = temp.next
            } while (temp != null && temp != head)
        }
    }
    
    override def find(ele: T): Node = {
        var temp = head
        while (temp != null) {
            if (temp.value == ele) {
                return temp
            }
            temp = temp.next
            // 查找的过程中, 重新回到了头部,则结束查找,没有找到
            if (temp == head) return null
        }
        null
    }
    
    override def delete(ele: T): Boolean = {
        
        if (super.delete(ele)) {
            // 重新构建环
            if (head != null) head.pre = tail  // 避免空指针, 加上非空判断
            if (tail != null) tail.next = head
            true
        } else false
    }
}