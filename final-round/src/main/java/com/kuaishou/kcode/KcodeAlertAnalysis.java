package com.kuaishou.kcode;

import java.io.FileNotFoundException;
import java.util.Collection;

/**
 * @author KCODE
 * Created on 2020-07-04
 */
public interface KcodeAlertAnalysis {

    String ALERT_TYPE_P99 = "P99";
    String ALERT_TYPE_SR = "SR";

    /**
     * 实现一个报警检查程序，输入一组报警规则和一个监控文件路径，触发器负责分析监控数据并返回所有的触发的报警的数据
     * 详细说明 参考readme
     *
     * @param path 需要分析文件的路径（绝对路径），由评测系统输入
     * @param alertRules 所有报警规则，由评测系统输入
     * @return 按照readme的说明返回报警信息集合，若没有监测到报警返回null
     */
    Collection<String> alarmMonitor(String path, Collection<String> alertRules) throws Exception;

    /**
     * @param caller 主调服务名称
     * @param responder 被调服务名称
     * @param time 报警发生时间（分钟），格式 yyyy-MM-dd hh:mm
     * @param type 报警类型，只能是 ALERT_TYPE_P99 或 ALERT_TYPE_SR
     * @return 返回在这一分钟内主被调按ip聚合的成功率和P99(按成功率倒排)，无调用反馈空list
     */
    Collection<String> getLongestPath(String caller, String responder, String time, String type);
}