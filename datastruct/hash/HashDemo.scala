



object HashDemo {
    def main(args: Array[String]): Unit = {
        val table = new HashTable[Int]
        table.add(10)
        table.add(10)
        table.add(20)
        table.printAll
    }
}

class HashTable[T] {
    val initSize = 2
    val arr = new Array[DoublyLinkedList[T]](initSize)
    
    def add(e: T) = {
        // 找到e应该去的那个链表所在数组中的的索引
        val index = e.hashCode().abs % initSize
        // 如果是一次在这个位置添加元素, 则应该先创建链表
        if(arr(index) == null) arr(index) = new DoublyLinkedList[T]
        
        arr(index).add(e)
    }
    
    def printAll = {
        for(i <- 0 until arr.length){
            val list = arr(i)
            print(s"$i : ")
            if(list != null){
                list.printAll
            }
            println()
        }
    }
}


/*
基本的数据结构:
数组
    优点
        元素的定位比较快, 有索引  O(1)
    缺点
        删除和添加比较慢 O(n)
        
链表
    优点
        插入和删除比较快  O(1)
    确定
        查找元素  O(n)

散列表或者hash表, 集合了数组和链表的优点

*/