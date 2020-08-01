package com.kuaishou.kcode.check.demo;

import static com.kuaishou.kcode.check.demo.Utils.createQ1CheckResult;
import static com.kuaishou.kcode.check.demo.Utils.createQ2Result;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import com.kuaishou.kcode.KcodeAlertAnalysis;
import com.kuaishou.kcode.KcodeAlertAnalysisImpl;
import com.kuaishou.kcode.KcodeAlertAnalysisImpl02;
import com.kuaishou.kcode.domain.PairData;

/**
 * @author KCODE
 * Created on 2020-07-01
 */
public class KcodeAlertAnalysisTest {

    private static int M = 69804;

    public static void main(String[] args) throws Exception {
        // 第一套数据集
        //kcodeAlertForStudent-1.data，原始监控数据
        String sourceFilePath1 = "/data/kcode/data1/kcodeAlertForStudent-1.data";
        // ruleForStudent-1，报警规则
        String ruleFilePath1 = "/data/kcode/data1/ruleForStudent-1.txt";
        // Q1Result-1.txt，第一问结果
        String q1ResultFilePath1 = "/data/kcode/data1/Q1Result-1.txt";
        // Q2Result-1.txt，第二问输出和结果
        String q2ResultFilePath1 = "/data/kcode/data1/Q2Result-1.txt";

        // 第二套数据集
        //kcodeAlertForStudent-2.data，原始监控数据
        String sourceFilePath2 = "/data/kcode/data2/kcodeAlertForStudent-2.data";
        // ruleForStudent-2，报警规则
        String ruleFilePath2 = "/data/kcode/data2/ruleForStudent-2.txt";
        // Q1Result-2.txt，第一问结果
        String q1ResultFilePath2 = "/data/kcode/data2/Q1Result-2.txt";
        // Q2Result-2.txt，第二问输出和结果
        String q2ResultFilePath2 = "/data/kcode/data2/Q2Result-2.txt";

        // 第三套小数据集
        String sourceFilePath3 = "/data/kcode/data3/kcodeAlertForStudent-test.data";
        String ruleFilePath3 = "/data/kcode/data3/ruleForStudent-test.txt";
        String q1ResultFilePath3 = "/data/kcode/data3/Q1Result-test.data";
        String q2ResultFilePath3 = "/data/kcode/data3/Q2Answer-test.data";

        long startMs = System.currentTimeMillis();
        System.out.println("*************************** DATASET1 ***************************");
        testQuestion12(sourceFilePath1, ruleFilePath1, q1ResultFilePath1, q2ResultFilePath1);
        System.out.println("*************************** DATASET2 ***************************");
        testQuestion12(sourceFilePath2, ruleFilePath2, q1ResultFilePath2, q2ResultFilePath2);
        System.out.println("*************************** DATASET3 ***************************");
        testQuestion12(sourceFilePath3, ruleFilePath3, q1ResultFilePath3, q2ResultFilePath3);
        System.out.println("**************************** FINISH ****************************");

        System.out.println("\n* ----------------------------------------------- *");
        System.out.println("total cast(ms): " + (System.currentTimeMillis() - startMs));
        System.out.println("used Memory: "
                + ( Runtime.getRuntime().maxMemory() / (1024 * 1024)
                - Runtime.getRuntime().freeMemory() / (1024 * 1024) )
                + "MB");
        System.out.println("* ----------------------------------------------- *\n");

    }

    public static void testQuestion12(String sourceFilePath, String ruleFilePath,
                                      String q1ResultFilePath, String q2ResultFilePath) throws Exception {
        // Q1
        Set<Q1Result> q1CheckResult = createQ1CheckResult(q1ResultFilePath);
        KcodeAlertAnalysis instance = new KcodeAlertAnalysisImpl();
        List<String> alertRules = Files.lines(Paths.get(ruleFilePath)).collect(Collectors.toList());
        long start = System.nanoTime();
        Collection<String> alertResult = instance.alarmMonitor(sourceFilePath, alertRules);
        long finish = System.nanoTime();

        if (Objects.isNull(alertResult) || alertResult.size() != q1CheckResult.size()) {
            System.out.println("Q1 Error Size: [my: " + alertResult.size() + ", ok: " + q1CheckResult.size() + "]");
            return;
        }
        Set<Q1Result> resultSet = alertResult.stream().map(Q1Result::new).collect(Collectors.toSet());
//        if (!resultSet.containsAll(q1CheckResult)) {
//            System.out.println("Q1 Error Value");
//            System.out.println(resultSet);
//            return;
//        }
        for(Q1Result rs : resultSet) {
            if(!q1CheckResult.contains(rs)) {
                System.out.println("Q1 Error Value: " + rs);
                System.out.println("my values: ");
                System.out.println(resultSet);
//                System.out.println("ok values: ");
//                System.out.println(q1CheckResult);
                return;
            }
        }

        System.out.println("Q1: " + NANOSECONDS.toMillis(finish - start) + "ms");

        // Q2
        Map<Q2Input, Set<Q2Result>> q2Result = createQ2Result(q2ResultFilePath);
        long cast = 0L;
        for (Map.Entry<Q2Input, Set<Q2Result>> entry : q2Result.entrySet()) {
            start = System.nanoTime();
            Q2Input q2Input = entry.getKey();
            Collection<String> longestPaths = instance.getLongestPath(q2Input.getCaller(), q2Input.getResponder(), q2Input.getTime(), q2Input.getType());
            finish = System.nanoTime();
            Set<Q2Result> checkResult = entry.getValue();

            if (Objects.isNull(longestPaths) || longestPaths.size() != checkResult.size()) {
                System.out.println("Q2 Input: " + q2Input);
                System.out.println("Q2 Error Size: [my: " + longestPaths.size() + ", ok: " + checkResult.size() + "]");
                return;
            }
            Set<Q2Result> results = longestPaths.stream().map(Q2Result::new).collect(Collectors.toSet());
            if (!results.containsAll(checkResult)) {
                System.out.println("Q2 Error Result:" + q2Input);
                return;
            }
            cast += (finish - start);
        }
        System.out.println("Q2: " + cast + "ns");

    }
}