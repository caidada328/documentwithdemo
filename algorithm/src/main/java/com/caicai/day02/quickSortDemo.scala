package com.atguiug.day02

object quickSortDemo {
  def main(args: Array[String]): Unit = {
    val array = Array(7, 4, 2, 3, 5)
    quickSort(array, 0, 4)
    println(array.toList)
  }

  def quickSort(array: Array[Int], start: Int, end: Int): Unit = {
    if (start < end) {
      var standardvalue = array(start)
      var low = start
      var high = end
      while (low < high) {

        while (low < high && array(high) >= standardvalue) {
          high -= 1
        }
        array(low) = array(high)
        while (low < high && array(low) < standardvalue) {
          low  += 1
        }
        array(high) = array(low)

      }
      array(high) = standardvalue
      quickSort(array, start, low - 1)
      quickSort(array, low + 1, end)
    }


  }
}
