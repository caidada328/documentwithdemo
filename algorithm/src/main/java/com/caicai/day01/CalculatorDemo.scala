package com.atguiug.day01

import com.atguiug.day01.Stack.ArrayStack

object CalculatorDemo {
  def main(args: Array[String]): Unit = {
  val calculator = new Calculator
    println(calculator.start("3-6*2.1"))
  }

  class Calculator{

    def start(expressiom:String)={

      val numStack = new ArrayStack[Double](1000)
      val notationStack = new ArrayStack[Char](1000)
      val expr = expressiom.replaceAll("\\s+","")
      val sb = new StringBuilder
      expr.foreach{
        case c if c.toString.matches("[\\d\\.]") => sb.append(c)
        case c =>{
          numStack.push(sb.toDouble)
          sb.clear()
          var isOver = false
          while(!isOver){
            val topNotation = notationStack.getPeak
            if(topNotation.isDefined && Iteq(c,topNotation.get)){

              val tepRes = calc(notationStack.pop.get,numStack.pop.get,numStack.pop.get)
              numStack.push(tepRes)
            }else{
              isOver = true
            }
          }
          notationStack.push(c)
        }
      }
      numStack.push(sb.toDouble)
      while(!notationStack.isEmpty){
        val tepRes = calc(notationStack.pop.get ,numStack.pop.get,numStack.pop.get)
        numStack.push(tepRes)
      }
      //返回对应Option 的值
      numStack.pop.get
    }

    def Iteq(notation1: Char, notation2: Char): Boolean =getPriority(notation1) <= getPriority(notation2)

    def calc(op:Char,num1:Double,num2:Double): Double ={
     op match {
       case '+' => num1 + num2
       case '-' => num1 - num2
       case '*' => num1 * num2
       case '/' => num1 / num2
     }
    }

    def getPriority(notation:Char):Int ={
      notation match {
        case '+' => 1
        case '-' => 1
        case '*' => 2
        case '/' => 2

      }
    }
  }
}
