package com.kuaishou.kcode;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;

/**
 * @author kcode
 * Created on 2020-05-20
 */
public class KcodeMain {

    /* 程序模块与计时类 */
    private static int MODULE_PREPARE = 0;      /* 准备数据 */
    private static int MODULE_GET_RESULT = 1;   /* 查询答案 */
    private static int MODULE_TOTAL = 2;        /* 总共有几个模块 */
    private static FlyanTimer timer = new FlyanTimer(MODULE_TOTAL);

    public static void main(String[] args) throws Exception {
        /* io-file settings */
        String INPUT_FILENAME = "/data/kcode/warmup-test.data";
        String OUTPUT_FILENAME = "/data/kcode/result-test.data";

        // "demo.data" 是你从网盘上下载的测试数据，这里直接填你的本地绝对路径
        InputStream fileInputStream = new FileInputStream(INPUT_FILENAME);
        Class<?> clazz = Class.forName("com.kuaishou.kcode.KcodeQuestion");
        Object instance = clazz.newInstance();
        Method prepareMethod = clazz.getMethod("prepare", InputStream.class);
        Method getResultMethod = clazz.getMethod("getResult", Long.class, String.class);
        // 调用prepare()方法准备数据
        timer.startTag(MODULE_PREPARE);
        prepareMethod.invoke(instance, fileInputStream);    /* 只会调用一次... */
        timer.endTag(MODULE_PREPARE);

        // 验证正确性
        // "result.data" 是你从网盘上下载的结果数据，这里直接填你的本地绝对路径
        System.out.println("verifying the answer......");
        timer.startTag(MODULE_GET_RESULT);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(OUTPUT_FILENAME)));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] split = line.split("\\|");     /* 切割 key 和 answer */
            String[] keys = split[0].split(",");    /* 切割 key */
            // 调用getResult()方法
            Object result = getResultMethod.invoke(instance, new Long(keys[0]), keys[1]);
            if (!split[1].equals(result)) {             /* 答案相同否？ */
                System.out.println("KcodeMain::check answer failed.\n");
                System.out.println("Key: [" + keys[1] + ", " + Integer.parseInt(keys[0]) + "]");
                System.out.println("ok answer: " + split[1]);
                System.out.println("my answer: " + result);
                System.exit(-1);
            }
        }
        timer.endTag(MODULE_GET_RESULT);

        printTimerInfos();
    }

    /**
     * 打印所有模块的耗时信息
     */
    private static void printTimerInfos() {
        System.out.println("* ----------------------------------------------- *");

        timer.printElapsed(MODULE_PREPARE, "PREPARE");

        timer.printElapsed(MODULE_GET_RESULT, "GET_RESULT");

        timer.printTotalElapsed();

        System.out.println("* ----------------------------------------------- *");

        System.out.println("freeMemory: " + Runtime.getRuntime().freeMemory() / (1024 * 1024));

        System.out.println("* ----------------------------------------------- *");
    }
}