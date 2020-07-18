
object QueueDemo {
    def main(args: Array[String]): Unit = {
        val queue = new ArrayQueue[Int](5)
        queue.enqueue(10)
        queue.enqueue(20)
        queue.enqueue(30)
        queue.enqueue(40)
        queue.enqueue(50)
//        queue.printQueue
        println(queue.dequeue())
        println(queue.dequeue())
        println(queue.dequeue())
//        queue.printQueue
        queue.enqueue(60)
        
        println("-------")
        queue.printQueue
    }
}
// 在jvm运行的时候, ClassTag可以记住泛型
class ArrayQueue[T: ClassTag](initSize: Int) {
    // 定义数组, 用来存储数据   如果要泛型数组,需要给T添加上下文
    val arr = new Array[T](initSize)   // new int new String
    // 队头(出队的时候, 这个位置的元素出去)
    var head = 0
    // 表示队列下一个元素的位置(入队的时候, 新的元素放在这个位置)
    var tail = 0
    // 队列中元素的个数 (用来判断队列是否为空, 或者满了)
    var count = 0
    
    // 判断队列是否为空
    def isEmpty = count == 0
    
    // 表示队列已经满了
    def isFull = count == initSize
    
    // 入队
    def enqueue(ele: T) = {
        // 1. 判断队列是否满了, 如果已经满了, 则无法入队
        if (isFull) throw new UnsupportedOperationException("队里已满, 无法添加元素!")
        // 2. 如果没有满, 则入队. 把新入队的元素放在tail的位置
        arr(tail) = ele
        // 2.2 tail应该更新(tail + 1)
        tail += 1
        // 2.3 队列中元素的个数也要+1
        count += 1
        
        // 已经到最后一个位置, 那下次的元素去数组头部
        if (tail == initSize) tail = 0
    }
    
    // 出队
    def dequeue() = {
        // 1. 判断队列是否空
        if (isEmpty) throw new UnsupportedOperationException("队里为空, 无法出队!")
        // 2. 把队头的位置出去
        val result = arr(head)
        head += 1
        // 总数减1
        count -= 1
        
        if (head == initSize) head = 0
        result
    }
    
    def printQueue = {
        var temp = head
        for(i <- 0 until count){
            println(arr(temp))
            temp += 1
            if(temp == initSize) temp = 0
        }
    }
}