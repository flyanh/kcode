package com.kuaishou.kcode;

import java.io.*;
import java.util.HashSet;
import java.util.List;

/**
 * @team 辰海飞燕
 * @member 陈元崇 - 济南大学泉城学院
 * @member 张浩宇 - 哈尔滨工业大学
 * @member 王俊杰 - 南京理工大学
 *
 * Created on 2020-06-17
 *
 *
 */

public class KcodeRpcMonitorImpl implements KcodeRpcMonitor {

    /* 答案 */
    private Answer answer = new Answer();

    /* 数据区域 */
    private HashSet<String> methodAndIpPairStringSet = new HashSet<>();
    private String[] methodAndIpPairStringArray = null;

    /* 构造函数 */
    public KcodeRpcMonitorImpl() {

    }

    /**
     * 线下数据集信息：
     *  nr_caller   nr_ip_caller    nr_responder    nr_ip_responder     scope_time_minute
     *  78          439             70              388                 30
     */
    public void prepare(String path) throws IOException {
        /* 初始化当前时间 */
        BufferedReader inputStream = new BufferedReader(new FileReader(path));
        int currTime = (int) (Long.parseLong(inputStream.readLine().split(",")[6]) / (1000 * 60));
        inputStream.close();

        /* 得到所有主调对 */
        inputStream = new BufferedReader(new FileReader(path));
        String line;
        String[] split;

        String methodAndIpPairString;
        String caller;      /* 调用方 */
        String callerIP;    /* 调用方 IP */
        String responder;      /* 被调用目标方 */
        String responderIP;    /* 被调用目标方 IP */
        boolean success;      /* 调用成功？ */
        int elapsedTime;    /* 调用耗时 */
        int startTime;      /* 调用开始时间 */
        int methodAndIpPairSize;
        boolean findOut = false;
        while ( !findOut ) {
            line = inputStream.readLine();
            /* 先转换 */
            split = line.split(",");
            caller = split[0];
            callerIP = split[1];
            responder = split[2];
            responderIP = split[3];
            success = Boolean.parseBoolean(split[4]);
            elapsedTime = Integer.parseInt(split[5]);
            startTime = (int) (Long.parseLong(split[6]) / (1000 * 60));   /* 按分钟算 */
            methodAndIpPairString = caller + responder + "|" + callerIP + "," + responderIP;

            if(startTime != currTime) {
                /* 将所有主被调对转换为数组 */
                methodAndIpPairSize = methodAndIpPairStringSet.size();
                methodAndIpPairStringArray
                        = methodAndIpPairStringSet.toArray(new String[methodAndIpPairSize]);

                /* 计算 */
                for(String mp : methodAndIpPairStringArray) {
                    answer.pairAnswerCompute(mp, currTime);
                }

                /* 更新当前时间 */
                currTime = startTime;
                methodAndIpPairStringSet.clear();
                findOut = true;
            }

            /* 将一个主被调对加入集合 */

            methodAndIpPairStringSet.add(methodAndIpPairString);
            /* 加入到输入数据区域 */
            answer.addPairData(methodAndIpPairString, elapsedTime, success);
            answer.addResponderData(responder, startTime, success);
        }

        /* 开始读取并计算 */
        while ( (line = inputStream.readLine()) != null ) {
            /* 先转换 */
            split = line.split(",");
            caller = split[0];
            callerIP = split[1];
            responder = split[2];
            responderIP = split[3];
            success = Boolean.parseBoolean(split[4]);
            elapsedTime = Integer.parseInt(split[5]);
            startTime = (int) (Long.parseLong(split[6]) / (1000 * 60));   /* 按分钟算 */
            methodAndIpPairString = caller + responder + "|" + callerIP + "," + responderIP;

            if(startTime != currTime) {
                /* 计算 */
                for(String mp : methodAndIpPairStringArray) {
                    answer.pairAnswerCompute(mp, currTime);
                }

                /* 更新当前时间 */
                currTime = startTime;
            }
            /* 加入到输入数据区域 */
            answer.addPairData(methodAndIpPairString, elapsedTime, success);
            answer.addResponderData(responder, startTime, success);
        }
        for(String mp : methodAndIpPairStringArray) {
            answer.pairAnswerCompute(mp, currTime);
        }
    }



    public List<String> checkPair(String caller, String responder, String time) {
        return answer.getPairAnswer(caller, responder, time);
    }



    public String checkResponder(String responder, String start, String end) {
        return answer.getResponderAnswer(responder, start, end);
    }

}
