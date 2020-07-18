
object Josephu {
    def main(args: Array[String]): Unit = {
        val last = startGame(60, 1, 3)
        println("\n" + last)
    }
    
    /**
     *
     * @param n     总共n个人
     * @param start 从第start个人开始数数
     * @param num   数到num的人拉出去枪毙
     */
    def startGame(n: Int, start: Int, num: Int) = {
        val list: CircleDoublyLinkedList[Int] = new CircleDoublyLinkedList[Int]
        // 1. 给链表初始化n个人(围成一圈)
        for (i <- 1 to n) {
            list.add(i)
        }
        
        var startNode: list.Node = list.find(start).pre
        while (list.head != list.tail) {
            for (i <- 1 to num) {
                startNode = startNode.next
            }
            // 找到好枪毙那个人, 就是startNode
            list.delete(startNode.value)
            print(startNode + "->")
            startNode = startNode.pre
        }
        startNode.value
    }
}
