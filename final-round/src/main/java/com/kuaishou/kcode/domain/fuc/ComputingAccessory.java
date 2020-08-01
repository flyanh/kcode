package com.kuaishou.kcode.domain.fuc;

import com.kuaishou.kcode.domain.PairData;

import java.util.Map;

/**
 * @author flyan
 * date 2020-07-15
 * @function 计算配件，包含了当前已经添加的所有数据以及时间信息提供给计算工厂进行计算
 */
public class ComputingAccessory {

    private Map<Integer, PairData> dataMap; /* 数据 */
    private int timeIndex;                  /* 时间分钟索引，数据来自该分钟 */

    public ComputingAccessory(Map<Integer, PairData> dataMap, int timeIndex) {
        this.dataMap = dataMap;
        this.timeIndex = timeIndex;
    }

    public Map<Integer, PairData> getDataMap() {
        return dataMap;
    }

    public ComputingAccessory setDataMap(Map<Integer, PairData> dataMap) {
        this.dataMap = dataMap;
        return this;
    }

    public int getTimeIndex() {
        return timeIndex;
    }

    public ComputingAccessory setTimeIndex(int timeIndex) {
        this.timeIndex = timeIndex;
        return this;
    }

    @Override
    public String toString() {
        return "ComputingAccessory{" +
                "timeIndex=" + timeIndex +
                '}';
    }
}
