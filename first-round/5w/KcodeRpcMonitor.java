package com.kuaishou.kcode;

import java.util.List;

/**
 * @author kcode
 * Created on 2020-06-01
 */
public interface KcodeRpcMonitor {

    /**
     * 接收和分析调用信息的接口
     *
     * @param path, 需要分析文件的路径（绝对路径），由评测系统输入
     */
    void prepare(String path) throws Exception;

    /**
     * @param caller 主调服务名称
     * @param responder 被调服务名称
     * @param time 需要查询的时间（分钟），格式 yyyy-MM-dd hh:mm
     * @return 返回在这一分钟内主被调按ip聚合的成功率和P99(按成功率倒排)，无调用反馈空list
     */
    List<String> checkPair(String caller, String responder, String time);

    /**
     * @param responder 被调服务名称
     * @param start 需要查询区间的开始时间（分钟），格式 yyyy-MM-dd hh:mm
     * @param end 需要查询区间的结束时间（分钟），格式 yyyy-MM-dd hh:mm
     * @return 返回在start，end区间内responder作为被调的平均成功率
     */
    String checkResponder(String responder, String start, String end);
}
