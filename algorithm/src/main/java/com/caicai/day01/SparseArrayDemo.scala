package com.atguiug.day01

import scala.collection.mutable.ArrayBuffer

object SparseArrayDemo {
  //初始化棋盘的大小

  val rowNum = 15
  val colNum = 15



  def main(args: Array[String]): Unit = {

    //1、初始化棋盘
    val chess:Array[Array[Int]] = initChessBoard()
  }

  //初始化一个15*15d的棋盘，使用二维数组来存储元素
  def initChessBoard(): Array[Array[Int]] = {
    //ofDim[Int](rowNum,colNum)  创建一个二维数组
    val arr = Array.ofDim[Int](rowNum,colNum)
    val white = 1
    val black = 2
    arr(1)(2) = white
    arr(2)(3) = black
    arr
  }

  def printChess(chess:Array[Array[Int]])={
    //indices是一个范围0 until length
    for(row <- chess.indices;col<- chess(row).indices){
      print(chess(row)(col) + " ")
      if(col == chess(row).length - 1)
        println()

    }

  }
  def sparse2Chess(sparseArray: SparseArray) = {
    val arr = Array.ofDim[Int](rowNum,colNum)
    sparseArray.buf.foreach(node =>{
      arr(node.row)(node.col) = node.value
    })
    arr
  }

  def Chesst2SparseArray (chess:Array[Array[Int]]):SparseArray ={
    val sparseArray = new SparseArray
    for(row <- chess.indices;col <- chess(row).indices if chess(row)(col) != 0){
      sparseArray.add(row,col,chess(row)(col))
    }
    sparseArray
  }

}

//定义一个稀疏数组，在一个ArrayBuffer中存储一个个的元素Node，三维的 坐标以及数值
class SparseArray{
  val buf:ArrayBuffer[Node] = ArrayBuffer[Node]()

  def isEmpty = buf.isEmpty

  def add(row:Int,col:Int,value:Int):ArrayBuffer[Node] ={
    buf += Node(row,col,value)
  }

  override  def toString = buf.toString


  case class Node (row:Int,col:Int,value:Int)
}