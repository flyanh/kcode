package com.kuaishou.kcode.domain;

/**
 * @author flyan
 * date 2020-07-06
 * @function 自写 Int ArrayList，它比库里的要快，我们只考虑我们所需要的
 *           为什么不封装一个用泛型的？因为 <T> 只能存放引用类型，返回数据
 *           也需要进行类型转换，这也是为了效率的妥协。
 */
public class FlyanIntArrayList {

    private final int initialCapacity = 1002;   /* 默认容量 */

    private int[] data;
    private int length;
    private int maxSize;

    /**
     * 使用默认容量创建一个 FlyanIntArrayList
     */
    public FlyanIntArrayList() {
        data = new int[initialCapacity];
        maxSize = initialCapacity;
    }

    /**
     * 指定容量创建
     *
     * @param initialCapacity 初始容量
     */
    public FlyanIntArrayList(int initialCapacity) {
        data = new int[initialCapacity];
        maxSize = initialCapacity;
    }

    /**
     * 添加一个元素
     * @param elem 元素
     */
    public void add(final int elem) {
        /* 是否需要扩容 */
        if(length == maxSize) {
            /* 扩容两倍，简单粗暴 */
            int[] newArray = new int[maxSize += length];
            /* 数据迁移 */
            System.arraycopy(data, 0, newArray, 0, length);
            data = newArray;
        }
        data[length++] = elem;
    }

    /**
     * 批量添加元素，要求给予一个数组
     *
     * @param from 包含新元素的数组
     * @param length 要添加多少？
     */
    public void addAll(int[] from, final int length) {
        /* 是否需要扩容 */
        int newLength = this.length + length;
        if(newLength >= maxSize) {
            /* 扩容到新长度 */
            maxSize = newLength + 1002;
            int[] newArray = new int[maxSize];
            /* 数据迁移 */
            System.arraycopy(data, 0, newArray, 0, this.length);
            data = newArray;
        }
        /* 接着拷贝所有要添加的数据 */
        System.arraycopy(from, 0, data, this.length, length);
        this.length = newLength;
    }

    public int get(int idx) {
        return data[idx];
    }

    public int size() {
        return length;
    }

    public void size(int size) {
        this.length = size;
    }

    public int[] getData() {
        return data;
    }
}
