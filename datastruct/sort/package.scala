

import scala.util.Random

package object sort {
    def swap(arr: Array[Int], i: Int, j: Int): Unit = {
        val tmp = arr(i)
        arr(i) = arr(j)
        arr(j) = tmp
    }
    
    def randomArr: Array[Int] = {
        val random = new Random()
        (1 to 2000).map(_ => random.nextInt(1000)).toArray
    }
}
