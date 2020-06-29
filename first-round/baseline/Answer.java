package com.kuaishou.kcode;

import javafx.util.Pair;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author flyan
 * date 2020-06-17
 * @function 管理答案
 */
public class Answer {

    /**
     * 一个主被调对的数据，包含耗时列表以及调用响应情况
     */
    static class PairData {
        List<Integer> elapsedTimes; /* 调用耗时列表 */
        int trueCount;              /* 调用成功次数 */
        int callTotal;              /* 调用总次数 */

        PairData() {
            this.elapsedTimes = new ArrayList<>();
        }
    }

    /**
     * 一个响应者(方法名)在某一时刻的响应情况
     */
    static class ResponderData {
        int trueCount;      /* 调用成功次数 */
        int callTotal;      /* 调用总次数 */
    }

    /* 查询 1 答案映射 */
    private Map<String, List<String>> pairAnswerMap = new HashMap<>();
    /* 查询 1 数据映射 */
    private Map<String, PairData> pairDataMap = new HashMap<>();

    /* 查询 2 答案映射 */
    private Map<String, String> responderAnswerMap = new HashMap<>();
    /* 查询 2 数据映射 */
    private Map<String, ResponderData> responderDataMap = new HashMap<>();

    /* 时间格式化对象 */
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    public Answer() {

    }

    /**
     * 添加此次的耗时以及调用响应情况到该主被调对数据中
     *
     * @param methodAndIpPairString 调用方法和 IP 对组合字符串
     * @param elapsedTime 此次调用耗时(ms)
     * @param success 调用成功否？
     */
    public void addPairData(final String methodAndIpPairString,
                            final int elapsedTime, final boolean success) {
        PairData pairData =
                this.pairDataMap.computeIfAbsent(methodAndIpPairString, k -> new PairData());
        pairData.elapsedTimes.add(elapsedTime); /* 耗时 */
        pairData.callTotal++;                   /* 调用总次数 */
        if(success) pairData.trueCount++;       /* 成功调用总次数 */
    }

    /**
     * 添加一个响应者在某一时刻(分)的数据
     *
     * @param responder 响应者
     * @param startTime 开始时间(分)
     * @param success 调用成功否？
     */
    public void addResponderData(final String responder,
                                 final int startTime, final boolean success) {
        final String responderOnStartTime = responder + startTime;
        ResponderData responderData =
                this.responderDataMap.computeIfAbsent(responderOnStartTime, k -> new ResponderData());
        responderData.callTotal++;              /* 调用总次数 */
        if(success) responderData.trueCount++;  /* 成功调用总次数 */
    }

    /**
     * 计算一个主被调对在某一时刻(分)的答案
     *
     * @param methodAndIpPairString 调用方法和 IP 对组合字符串
     * @param startTime 开始时间(分)
     */
    public void pairAnswerCompute(final String methodAndIpPairString, int startTime) {
        PairData data = this.pairDataMap.get(methodAndIpPairString);
        if(data.callTotal == 0) return;    /* 本次时间该主被调对未出现调用 */

        /* 首先排序 */
        List<Integer> elapsedTimes = data.elapsedTimes;
        Collections.sort(elapsedTimes);

        /* P99 */
        int P99 = elapsedTimes.get((int) Math.ceil(elapsedTimes.size() * 0.99) - 1);

        /* 调用成功率，截取后两位小数，不进位 */
        double SR = Math.floor((double) data.trueCount / (double) data.callTotal * 10000) / 100;

        /* 存入答案 */
        String[] methodAndIpPair = methodAndIpPairString.split("\\|");
        String key = methodAndIpPair[0] + dateFormat.format((long)startTime * 60 * 1000);
        String SRS;
        if(SR == 0.0) SRS = ".00%"; /* 0% ？那么直接是 ".00%" */
        else {
            SRS = String.format("%.2f%%", SR);
            if(SR < 1.0) {          /* 在 (0%, 1%) 开区间？截断前面的 "0"，例如 "0.75%"，那么截断为 ".75%" */
                SRS = SRS.substring(0, 0) + SRS.substring(1);
            }
        }
        List<String> list = this.pairAnswerMap.computeIfAbsent(key, k -> new ArrayList<>());
        list.add(methodAndIpPair[1] + "," + SRS + "," + P99);

        /* 为了复用 */
        elapsedTimes.clear();
        data.trueCount = data.callTotal = 0;

    }

    /**
     * 计算响应者在某个时间范围内的答案
     *
     * @param responder 响应者
     * @param start 开始时间(分)
     * @param end 结束时间(分)
     * @param responderAndTimeScope 响应者与时间范围组合字符串，作为答案的 key
     * @return 答案字符串(平均成功率)
     */
    public String responderAnswerCompute(String responder, String start, String end,
                                         final String responderAndTimeScope) {
        double SR, TSR = 0;
        int totalExist = 0;
        ResponderData data;
        String AVG_RS_STRING = null;
        try {
            /* 得到开始时间戳(分)和结束时间戳(分) */
            int startTime = (int) (dateFormat.parse(start).getTime() / (1000 * 60));
            int endTime = (int) (dateFormat.parse(end).getTime() / (1000 * 60));

            /* 遍历闭区间内时间范围的所有调用情况并计算平均成功率 */
            while ( startTime <= endTime ) {
                /* 计算当前时间的成功率 */
                data = this.responderDataMap.get(responder + startTime);
                SR = 0;             /* 假设该分钟不存在调用 */
                if(data != null) {
                    SR = Math.floor((double) data.trueCount / (double) data.callTotal * 10000) / 100;
                    totalExist++;   /* 当前分钟存在调用 */
                }

                /* 加入到总成功率 */
                TSR += SR;

                /* 下一分钟 */
                startTime++;
            }

            /* 好的，现在计算平均成功率 */
            AVG_RS_STRING = "-1.00%";
            if(totalExist > 0) {
                SR = Math.floor(TSR / totalExist * 100) / 100;

                /* 转换为字符串 */
                AVG_RS_STRING = String.format("%.2f%%", SR);
            }

            /* 存入答案 */
            this.responderAnswerMap.put(responderAndTimeScope, AVG_RS_STRING);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return (totalExist > 0 ? AVG_RS_STRING : "-1.00%");
    }

    /**
     * 获取一个主被调对在某一时刻(分)的答案
     *
     * @param caller 调用者
     * @param responder 响应者
     * @param time 时间
     * @return 答案
     */
    public List<String> getPairAnswer(String caller, String responder, String time) {
        String key = caller + responder + time;
        List<String> ans = this.pairAnswerMap.get(key);
        return (ans != null ? ans : new ArrayList<>());
    }

    /**
     * 获取一个响应者在某个时间范围内的答案(平均调用成功率)
     *
     * @param responder 响应者
     * @param start 开始时间(分)
     * @param end 结束时间(分)
     * @return 答案
     */
    public String getResponderAnswer(String responder, String start, String end) {
        /* 如果该区间未被计算过，先进行计算 */
        final String key = responder + start + end;
        String ans = this.responderAnswerMap.get(key);
        if(ans == null) {
            ans = this.responderAnswerCompute(responder, start, end, key);
        }
        return ans;
    }

}
