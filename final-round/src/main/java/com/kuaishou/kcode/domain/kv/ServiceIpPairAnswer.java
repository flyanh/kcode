package com.kuaishou.kcode.domain.kv;

import java.util.ArrayList;
import java.util.List;

/**
 * @author flyan
 * date 2020-07-07
 * @function 主被调 IP 对一分钟的答案
 */
public class ServiceIpPairAnswer {

    /* 一个答案 */
    public static class Answer {
        private int P99;
        private int SR;
        private int callerIp;
        private int responderIp;

        public Answer(int p99, int SR, int callerIp, int responderIp) {
            P99 = p99;
            this.SR = SR;
            this.callerIp = callerIp;
            this.responderIp = responderIp;
        }

        public int getP99() {
            return P99;
        }

        public int getSR() {
            return SR;
        }

        public int getCallerIp() {
            return callerIp;
        }

        public int getResponderIp() {
            return responderIp;
        }
    }

    private List<Answer> answers = new ArrayList<>();   /* 一分钟内所有的答案 */
    private int maxP99;                                 /* 一分钟内最大 P99 */
    private int minP99 = Integer.MAX_VALUE;             /* 一分钟内最小 P99 */
    private int maxSR;                                  /* 一分钟内最大 SR */
    private int minSR = Integer.MAX_VALUE;              /* 一分钟内最小 SR */

    /**
     * 添加一条答案
     *
     * @param answer 答案
     */
    public void add(Answer answer) {
        answers.add(answer);
        /* 维护最大值 */
        if(answer.getP99() > maxP99) {
            maxP99 = answer.getP99();
        }
        if(answer.getSR() > maxSR) {
            maxSR = answer.getSR();
        }
        /* 维护最小值 */
        if(minP99 > answer.getP99()){
            minP99 = answer.getP99();
        }
        if(minSR > answer.getSR()){
            minSR = answer.getSR();
        }
    }

    public List<Answer> getAnswers() {
        return answers;
    }

    public int getMaxP99() {
        return maxP99;
    }

    public int getMinP99() {
        return minP99;
    }

    public int getMaxSR() {
        return maxSR;
    }

    public int getMinSR() {
        return minSR;
    }

    public int size() {
        return answers.size();
    }

}
