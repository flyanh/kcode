package com.kuaishou.kcode.domain;

import java.util.Random;

/**
 * @author flyan
 * date 2020-07-06
 * @function 一对(主被调对、IP 对)的数据，包含耗时列表以及调用响应情况
 */
public class PairData {

    private FlyanIntArrayList elapsedTimes;    /* 耗时列表 */
    private int trueCount;                  /* 调用成功次数 */

    private int P99 = -1;
    private int SR = -1;
    private Random random = new Random();

    public PairData() {
        elapsedTimes = new FlyanIntArrayList();
    }

    /**
     * 添加一组数据
     */
    public void add(final int elapsedTime, final int success) {
        elapsedTimes.add(elapsedTime);
        trueCount += success;
    }

    /**
     * 批量添加元素，要求给予一对数据
     *
     * @param from 另一对数据
     */
    public void addAll(PairData from) {
        elapsedTimes.addAll(from.elapsedTimes.getData(), from.size());
        trueCount += from.trueCount;
    }

    /**
     * 获取耗时列表的 P99 数据
     */
    public int getP99() {
        int size = elapsedTimes.size();
        return topkByQuickSort(elapsedTimes.getData(), 0, size - 1, (int) (size * 0.01));
    }

    public int getSuccessRate() {
        SR = trueCount * 10000 / elapsedTimes.size();
        /* 重置数据区域，为下次的计算做准备 */
        trueCount = 0;
        elapsedTimes.size(0);
        return SR;
    }

    public int getSafeP99() {
        if(P99 == -1) {
            int size = elapsedTimes.size();
            P99 = topkByQuickSort(elapsedTimes.getData(), 0, size - 1, (int) (size * 0.01));
        }
        return P99;
    }

    /**
     * 安全的获取成功率，在可能会产生溢出时，一定要使用这个函数，而不是上面那个，而且这个方法并不会重置整个数据区域
     */
    public int getSafeSuccessRate () {
        if(SR == -1){
            SR = (int) (trueCount * 10000L / elapsedTimes.size());
        }
        return SR;
    }

    public int size() {
        return elapsedTimes.size();
    }

    /**
     * 快排版 topk，进行从大到小排序，取 100% - k 位置停止。
     */
    private int topkByQuickSort(int[] a, int l, int r, int k) {
        int i = random.nextInt(r - l + 1) + l, j;
        int tmp = a[i];
        a[i] = a[r];
        a[r] = tmp;
        int x = a[r];
        for (j = l, i = l - 1; j < r; ++j) {
            if (a[j] > x) {
                tmp = a[++i];
                a[i] = a[j];
                a[j] = tmp;
            }
        }
        tmp = a[++i];
        a[i] = a[r];
        a[r] = tmp;
        if (i == k) {
            return a[i];
        } else {
            return i < k ? topkByQuickSort(a, i + 1, r, k) : topkByQuickSort(a, l, i - 1, k);
        }
    }

    public void printData() {
        System.out.print("data: [");
        for(int i = 0; i < elapsedTimes.size(); ++i) {
            System.out.print(elapsedTimes.get(i) + ", ");
        }
        System.out.println("]");
    }
}
