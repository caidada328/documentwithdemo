package com.datastruct.binarytree


object BinaryTreeDemo {
    def main(args: Array[String]): Unit = {
        val root = new BinaryTree[Int](10)
        root.isRoot = true
        root.left = new BinaryTree[Int](9)
        root.right = new BinaryTree[Int](20)
        root.right.left = new BinaryTree[Int](15)
        root.right.right = new BinaryTree[Int](35)
        
//                root.preForeach(x => println(x))
//                root.infixForeach(x => println(x))
        root.postForeach(x => println(x))
    }
}

// 表示一个二叉树
class BinaryTree[T](var value: T) {
    // 表示这个节点是否为root
    var isRoot: Boolean = false
    
    // 左子树
    var left: BinaryTree[T] = _
    // 右子树
    var right: BinaryTree[T] = _
    
    /**
     * 前序(先序)遍历
     * 当前节点->左->右
     *
     * @param op
     */
    def preForeach(op: T => Unit): Unit = {
        op(value) // 当前节点
        if (left != null) left.preForeach(op)
        if (right != null) right.preForeach(op)
    }
    
    /**
     * 中序遍历
     *
     * @param op
     */
    def infixForeach(op: T => Unit): Unit = {
        if (left != null) left.infixForeach(op)
        op(value) // 当前节点
        if (right != null) right.infixForeach(op)
    }
    
    /**
     * 中序遍历
     *
     * @param op
     */
    def postForeach(op: T => Unit): Unit = {
        if (left != null) left.postForeach(op)
        if (right != null) right.postForeach(op)
        op(value) // 当前节点
    }
}

/*
如何遍历一个二叉树:
前序遍历
    当前节点-> 左-> 右
中序遍历
    左-> 当前节点-> 右
后序遍历
    左->右->当前节点
 */
