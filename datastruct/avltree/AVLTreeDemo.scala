package com.datastruct.avltree


object AVLTreeDemo {
    def initTree() = {
        //        val arr = Array(10, 4, 16, 1, 20, 7, 8, 6, 9) // 右右 -> 左旋
        //        val arr = Array(4, 3, 6, 5, 7, 8) // 右右 -> 左旋
        //        val arr = Array(10, 12, 8, 9, 7, 6) // 右右 -> 左旋
        //        val arr = Array(30, 20, 40, 25, 50, 13, 15, 10, 8) // 右右 -> 左旋
//        val arr: Array[Int] = Array(10, 11, 7, 6, 8, 9) // (左右) 先左旋再右旋
        val arr: Array[Int] = Array(10, 8, 16, 18, 14, 12) // (右左) 先右旋再左旋
        val tree = new AVLTree[Int]()
        
        arr.foreach(x => tree.add(x))
        
        tree
    }
    
    def main(args: Array[String]): Unit = {
        val tree = initTree()
        println(tree.height)
        tree.infixForeach(x => print(x + "->"))
    }
    
    
}

/**
 * 排序二叉树
 *
 * @tparam T
 */
class AVLTree[T: Ordering] {
    // 冥界召唤Ordering[T]的隐式值
    private val ord: Ordering[T] = implicitly[Ordering[T]]
    
    var root: AVLNode[T] = _
    
    def search(v: T): AVLNode[T] = {
        // 空树, 直接返回null
        if (root == null) null
        else root.search(v)
    }
    
    /**
     * 给树添加节点
     *
     * @param v
     */
    def add(v: T) = {
        if (root == null) { // 如果第一次添加,自动成为根节点
            root = new AVLNode[T](v)
        } else {
            root.add(v)
            if (root.p != null) {
                root = root.p
            }
        } // 如果要添加的节点不是root, 交给root节点去完成
    }
    
    /**
     * 删除树中的节点
     *
     * @param v
     * @return 成功或失败
     */
    def delete(v: T): Boolean = {
        if (root == null) false
        else if (ord.equiv(v, root.value)) { // 删除的是根节点
            if (root.left == null && root.right == null) { // 只有跟节点
                root = null
            } else if (root.left != null && root.right != null) { // 左右节点都在
                // 删除右子树中最小的节点, 把最小的那个值赋值给root节点
                root.value = root.right.deleteMin()
            } else if (root.left != null && root.right == null) { // 左在右不在
                // 让root指向root.left, 现在的root的父节点指向null
                root = root.left
                root.p = null
            } else { // 左不在右在
                root = root.right
                root.p = null
            }
            true
        } else {
            root.delete(v)
        }
    }
    
    def infixForeach(op: T => Unit): Unit = {
        if (root != null) {
            if (root.left != null) root.left.infixForeach(op)
            op(root.value)
            if (root.left != null) root.right.infixForeach(op)
        }
    }
    
    def height = if (root == null) -1 else root.height
}

/**
 * 表示二叉排序树中的一个节点
 *
 * @param value
 * @tparam T
 */
class AVLNode[T: Ordering](var value: T) {
    // 冥界召唤Ordering[T]的隐式值
    private val ord: Ordering[T] = implicitly[Ordering[T]]
    
    // 左节点
    var left: AVLNode[T] = _
    // 右节点
    var right: AVLNode[T] = _
    // 父节点
    var p: AVLNode[T] = _
    
    def search(v: T): AVLNode[T] = {
        // 1. 先判断当前节点是否是要找的节点
        // 2. 如果不是, 判断是否小于当前节点的, 如果是, 则去left找
        // 3. 否则去右边找
        if (ord.equiv(v, value))
            this
        else if (ord.lt(v, value) && left != null)
            left.search(v)
        else if (ord.gt(v, value) && right != null)
            right.search(v)
        else
            null
    }
    
    /**
     * 添加节点
     *
     * @param v
     * @return
     */
    def add(v: T): Boolean = {
        if (ord.lteq(v, value)) { // 要么添加到左子树: 要添加的值小于等于当前root节点的值
            if (left == null) { // 如果左节点是null, 则新添加的值直接成为左节点
                left = new AVLNode(v)
                left.p = this // 左节点的父节点是当前节点
            }
            else left.add(v) // 否则交个左节点继续处理
        } else { // 要么添加到右子树: 要添加的值大于当前root节点的值
            if (right == null) {
                right = new AVLNode(v)
                right.p = this
            }
            else right.add(v)
        }
        //        println("旋转前...")
        rotate()
        //        println("旋转后...")
        true
    }
    
    def rotate(): Unit = {
        // 左左 -> 右旋
        // 当前左树高度 - 右树的告诉 > 1 并且  左子树的 左 - 右 > 0
        if (leftHeight - rightHeight > 1 && left.leftHeight - left.rightHeight > 0) {
            println("左左..." + value)
            println("平衡前: " + height)
            rightRotate()
            println("平衡后: " + height)
            return
        }
        
        // 右右 -> 左旋
        // 当前左树高度 - 右树的告诉 < -1 并且  有子树的 左 - 右 < 0
        if (leftHeight - rightHeight < -1 && right.leftHeight - right.rightHeight < 0) {
            println("右右..." + value)
            println("平衡前: " + height)
            leftRotate()
            println("平衡后: " + height)
            return
        }
        
        // 左右 -> 左旋, 右旋
        // 当前节点左边高, 当前节点的左节点是右边高
        if (leftHeight - rightHeight > 1 && left.leftHeight - left.rightHeight < 0) {
            println("左右..." + value)
            // 失衡的节点的左节点左旋
            left.leftRotate()
            // 当前节点左右旋
            rightRotate()
            return
        }
        
        // 右左 -> 右旋, 左旋
        if (leftHeight - rightHeight < -1 && right.leftHeight - right.rightHeight > 0) {
            println("右左..." + value)
            // 失衡的节点的左节点左旋
            right.rightRotate()
            // 当前节点左右旋
            leftRotate()
            return
        }
        
        
    }
    
    def rightRotate() = {
        // 1. 先缓存需要变换的节点
        val tempP: AVLNode[T] = p
        val tempLeft: AVLNode[T] = left
        val tempLeftRight: AVLNode[T] = left.right // 有可能不存在
        // 2. 进行旋转
        //2.1 让当前的节点的左节点, 指左节点的右节点
        left = tempLeftRight
        // 2.2 当左节点的右节点指向当前节点
        tempLeft.right = this
        // 2.3 当当前父节点的左(右)节点指向当前节点的左节点
        if (tempP != null && tempP.left == this) {
            tempP.left = tempLeft
        } else if (tempP != null) {
            tempP.right = tempLeft
        }
        // 2.4 更新父节点
        if (tempLeftRight != null) tempLeftRight.p = this
        this.p = tempLeft
        tempLeft.p = tempP
    }
    
    def leftRotate() = {
        // 1. 先缓存需要变换的节点
        val tempP: AVLNode[T] = p
        val tempRight: AVLNode[T] = right
        val tempRightLeft: AVLNode[T] = right.left
        // 2. 进行旋转
        // 2.1 让当前节点的右节点指向原来右节点的左节点
        right = tempRightLeft
        // 2.2 让原来的右节点的左节点指向当前节点
        tempRight.left = this
        // 2.3 让父节点的左节点指向原来的右节点
        if (tempP != null && tempP.left == this) { // 当前节点是它爹的左节点
            tempP.left = tempRight
        } else if (tempP != null) {
            tempP.right = tempRight
        }
        // 2.4 建立父节点关系
        if (tempRightLeft != null) tempRightLeft.p = this
        this.p = tempRight
        tempRight.p = tempP
    }
    
    /**
     * 计算树的高度
     */
    def height: Int = leftHeight.max(rightHeight) + 1
    
    /**
     * 左树高度
     *
     * @return
     */
    def leftHeight: Int = if (left == null) -1 else left.height
    
    /**
     * 右树高度
     *
     * @return
     */
    def rightHeight: Int = if (right == null) -1 else right.height
    
    
    /**
     * 删除节点
     *
     * @param v
     * @return
     */
    def delete(v: T): Boolean = {
        // 1. 当前节点就是要删除的
        // 2. 要删除的小于当前节点
        // 3. 要删除的大于当前节点
        if (ord.equiv(v, value)) {
            // 1. 如果删除的是叶子节点
            // 2. 如果是删除的节点的左右都在
            // 3. 如果删的节点的左右只有一方
            
            // a: 判断当前节点是他父亲的左儿子还是右儿子
            var isLeft = true // 假设是左儿子, 默认是true
            // 父节点不为空, 并且当前节点等于他父亲的右儿子  eq比较地址值
            if (p != null && this.eq(p.right)) { // 判断是否为右儿子
                isLeft = false
            }
            // b: 判断当前节点是否为叶子节点
            if (this.left == null && this.right == null) {
                if (isLeft) p.left = null
                else p.right = null
            } else if (this.left != null && this.right != null) {
                this.value = this.right.deleteMin()
            } else { // 要么有左, 要么有右    一定一个是null
                // 先找到非空的子节点
                val nonNullChildNode = if (left != null) left else right
                // 让非空子节点的父节点执行当前节点的父节点
                nonNullChildNode.p = p
                // 让p的left或者right指向nonNullChildNode
                if (isLeft) p.left = nonNullChildNode
                else p.right = nonNullChildNode
            }
            true
        } else if (ord.lt(v, value)) {
            if (left == null) false // 表示删的元素不存在
            else left.delete(v)
        } else {
            if (right == null) false // 表示删的元素不存在
            else right.delete(v)
        }
    }
    
    /**
     * 删除最小的节点
     *
     * @return
     */
    def deleteMin(): T = {
        var minNode = this
        while (minNode.left != null) {
            minNode = minNode.left
        }
        // 调用节点的删除方法进行删除
        minNode.delete(minNode.value)
        // 返回最小值
        minNode.value
    }
    
    /**
     * 中序遍历
     * 排序二叉树来说, 只要中序遍历才有意义
     *
     * @param op
     */
    def infixForeach(op: T => Unit): Unit = {
        if (left != null) left.infixForeach(op)
        op(value)
        if (right != null) right.infixForeach(op)
    }
    
    override def toString: String = s"value = $value"
}


/*
为了方便处理root节点, 这次把树和节点分开

 */