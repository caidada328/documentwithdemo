
object SearchBinaryTreeDemo {
    def initTree() = {
        val arr = Array(50, 60, 70, 58, 57, 40, 45, 30, 20, 80)
        val tree = new SearchBinaryTree[Int]()
        
        arr.foreach(x => tree.add(x))
        
        tree
    }
    
    def main(args: Array[String]): Unit = {
        val tree = initTree()
        tree.infixForeach(x => print(x + "->"))
        println()
        tree.delete(58)
        tree.infixForeach(x => print(x + "->"))
        println()
        println(tree.root.value)
        
    }
    
    
}

/**
 * 排序二叉树
 *
 * @tparam T
 */
class SearchBinaryTree[T: Ordering] {
    // 冥界召唤Ordering[T]的隐式值
    private val ord: Ordering[T] = implicitly[Ordering[T]]
    
    var root: TreeNode[T] = _
    
    def search(v: T): TreeNode[T] = {
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
            root = new TreeNode[T](v)
        } else root.add(v) // 如果要添加的节点不是root, 交给root节点去完成
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
               root.value =  root.right.deleteMin()
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
}

/**
 * 表示二叉排序树中的一个节点
 *
 * @param value
 * @tparam T
 */
class TreeNode[T: Ordering](var value: T) {
    // 冥界召唤Ordering[T]的隐式值
    private val ord: Ordering[T] = implicitly[Ordering[T]]
    
    // 左节点
    var left: TreeNode[T] = _
    // 右节点
    var right: TreeNode[T] = _
    // 父节点
    var p: TreeNode[T] = _
    
    def search(v: T): TreeNode[T] = {
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
                left = new TreeNode(v)
                left.p = this // 左节点的父节点是当前节点
            }
            else left.add(v) // 否则交个左节点继续处理
        } else { // 要么添加到右子树: 要添加的值大于当前root节点的值
            if (right == null) {
                right = new TreeNode(v)
                right.p = this
            }
            else right.add(v)
        }
        true
    }
    
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
        if(ord.equiv(v, value)){
            // 1. 如果删除的是叶子节点
            // 2. 如果是删除的节点的左右都在
            // 3. 如果删的节点的左右只有一方
            
            // a: 判断当前节点是他父亲的左儿子还是右儿子
            var isLeft = true // 假设是左儿子, 默认是true
            // 父节点不为空, 并且当前节点等于他父亲的右儿子  eq比较地址值
            if(p != null && this.eq(p.right) ) {  // 判断是否为右儿子
                isLeft = false
            }
            // b: 判断当前节点是否为叶子节点
            if(this.left == null && this.right == null){
                if(isLeft) p.left = null
                else p.right = null
            }else if(this.left != null && this.right != null){
                this.value = this.right.deleteMin()
            }else{  // 要么有左, 要么有右    一定一个是null
                // 先找到非空的子节点
                val nonNullChildNode = if(left != null) left else right
                // 让非空子节点的父节点执行当前节点的父节点
                nonNullChildNode.p = p
                // 让p的left或者right指向nonNullChildNode
                if(isLeft) p.left = nonNullChildNode
                else p.right = nonNullChildNode
            }
            true
        }else if(ord.lt(v, value)){
            if(left == null) false  // 表示删的元素不存在
            else left.delete(v)
        }else{
            if(right == null) false  // 表示删的元素不存在
            else right.delete(v)
        }
    }
    
    /**
     * 删除最小的节点
     * @return
     */
    def deleteMin(): T = {
        var minNode = this
        while(minNode.left != null){
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