package com.atguiug.day03;

public class HeapSort {
    static int num = 0;

    public static void main(String[] args) {
        HeapSort heapSort = new HeapSort();
        int[] tree = {4, 7, 2, 5, 1, 3, 8, 6, 9, 10, 16, 15, 13, 14, 11, 12};
//        int[] tree = {4, 7, 2, 5};
        int[] result = new int[tree.length];
        // 完全二叉树的高度为log以2为底n的对数，n为结点数
        // 执行log以2为底n的对数次，因为每次一个深度的堆化，完成所有高度的堆化即需要执行高度响应的次数
        // 递归次数为总结点数n/2
        // 所以此次for循环时间复杂度为【log以2为底n的对数 * n/2】
        for (int i = 0; i < Math.log(tree.length) / Math.log(2); i++) {
            heapSort.heapify(tree, tree.length - 1, (tree.length / 2 - 1));
        }
        // 而此次for循环时间复杂度也为【log以2为底n的对数 * n/2】
        for (int i = 0; i < tree.length; i++) {
            result[i] = tree[0];
            System.out.print(result[i] + " ");
            heapSort.swap(tree, 0, tree.length - 1 - i);
            heapSort.easyHeapify(tree, tree.length - 1 - i, 0);
        }
        // 所以综合以上此种堆排序的时间复杂度O为【log以2为底n的对数 * n】
        System.out.println();
        System.out.print(num);
    }

    // 初始堆化后简易堆化
    // 递归log以2为底n的对数次，因为每次进入一个深度，但是深度随着数组不断的完善长度减少，
    // 相应深度减少平均即为二分之一也就是【二分之一 * log以2为底n的对数次】
    public void easyHeapify(int[] tree, int n, int first) {
        num++;
        int left = 2 * first + 1;
        int right = 2 * first + 2;
        if (left >= n) {
            return;
        }
        if (tree[left] > tree[right] || right == n) {
            swap(tree, first, left);
            easyHeapify(tree, n, left);
        } else {
            swap(tree, first, right);
            easyHeapify(tree, n, right);
        }
    }

    // 使堆化
    // 递归次数为总结点数n/2
    public void heapify(int[] tree, int n, int father) {
//        num++;
        if (father < 0) {
            return;
        }
        int left = 2 * father + 1;
        int right = 2 * father + 2;
        int max = father;
        if (left <= n && tree[left] > tree[max]) {
            max = left;
        }
        if (right <= n && tree[right] > tree[max]) {
            max = right;
        }
        if (max != father) {
            swap(tree, max, father);
            heapify(tree, n, father - 1);
        } else {
            heapify(tree, n, father - 1);
        }
    }

    // 数字交换
    public void swap(int[] tree, int i, int j) {
        int temp = tree[i];
        tree[i] = tree[j];
        tree[j] = temp;

    }
}
