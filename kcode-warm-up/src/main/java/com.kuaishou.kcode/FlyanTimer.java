package com.kuaishou.kcode;

/**
 * @author flyan
 * date 2020-06-02
 * @function 计时器，用于调试
 */
public class FlyanTimer {

    private long[] startTimes;
    private long[] endTimes;
    int module_total;

    public FlyanTimer(int module_total) {
        this.module_total = module_total;
        startTimes = new long[module_total];
        endTimes = new long[module_total];
    }

    /**
     * 记录某模块的开始时间
     * @param idx 模块索引
     */
    void startTag(int idx) {
        startTimes[idx] = System.currentTimeMillis();
    }

    /**
     * 记录某模块的结束时间
     * @param idx 模块索引
     */
    void endTag(int idx) {
        endTimes[idx] = System.currentTimeMillis();
    }

    /**
     * 打印某模块的耗时
     * @param idx 模块索引
     * @param info 打印信息
     */
    void printElapsed(int idx, String info) {
        System.out.println(
                info + ": " +
                        (endTimes[idx] - startTimes[idx]) +
                        " ms");
    }

    /**
     * 打印总和时间
     */
    void printTotalElapsed() {
        long totalTime = 0;
        for(int i = this.module_total - 1; i >= 0; --i)
            totalTime += endTimes[i] - startTimes[i];
        System.out.println(
                "\nTOTAL: " +
                        totalTime + " ms");
    }

}
