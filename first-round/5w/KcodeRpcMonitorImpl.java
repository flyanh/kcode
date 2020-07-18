package com.kuaishou.kcode;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * @team 辰海飞燕
 * @member 陈元崇 - 济南大学泉城学院
 * @member 张浩宇 - 哈尔滨工业大学
 * @member 王俊杰 - 南京理工大学
 *
 * Created on 2020-06-17
 */

@SuppressWarnings("StatementWithEmptyBody")
public class KcodeRpcMonitorImpl implements KcodeRpcMonitor {

    /* ---------------- 魔数 ---------------- */
    private final int NR_RESPONDER = 70;         /* 响应者个数 */
    private final int NR_MINUTE = 30;            /* 时间(分)跨度 */
    private final int NR_IP_PAIR = 3821;         /* IP 对数量 */
    private final int METHOD_ENCODE_MAX = 210;   /* 方法编码的最大值 */
    private final int maxMinuteIndex = NR_MINUTE - 1;   /* 最大时间索引 */
    private int originTimeOfMagic;                      /* 第一分钟的魔数，它能让我们不用 - '0' 去计算分钟 */

    /* ---------------- 全局 ---------------- */

    /* ---------------- 多线程 ---------------- */
    private final static int NR_THREADS = 8;                 /* 最大线程数量，线下 8C4G */

    /* ---------------- 答案 ---------------- */
    /* 查询 1 答案映射 */
    private List<String>[][] pairAnswerArray = new List[METHOD_ENCODE_MAX * METHOD_ENCODE_MAX][NR_MINUTE];
    private List<String> pairErrorAnswer = new ArrayList<>();       /* 查询 1 错误答案 */
    /* 查询 1 数据映射区域 */
    private Map<Integer, PairData>[] pairDataMapArray = new Map[NR_THREADS];    /* 每个线程的数据区域 */
    private Map<Integer, PairData>[] taskDeltaPairData = new Map[NR_THREADS];   /* 每个线程的碎片区域 */

    /* 查询 2 答案映射 */
    private String[][] responderAnswerArray = new String[METHOD_ENCODE_MAX][NR_MINUTE * NR_MINUTE];
    /* 查询 2 数据映射区域 */
    private Set<Integer> responderSet = new HashSet<>(NR_RESPONDER);/* 响应者集合 */
    private int[][] trueCount = new int[METHOD_ENCODE_MAX][NR_MINUTE];            /* 成功调用次数 */
    private int[][] callCount = new int[METHOD_ENCODE_MAX][NR_MINUTE];            /* 调用总次数 */
    private int[][] successRate = new int[METHOD_ENCODE_MAX][NR_MINUTE];          /* 正确率，为了保留精确两位，乘与 10000 */
    private String responderErrorAnswer = "-1.00";                  /* 查询 2 错误答案 */

    /* ---------------- 答案缓存区域 ---------------- */
    private List<String>[] pairAnswerCache = new List[5000];
    private int pairAnswerCrEncodeCacheHead;
    private int pairAnswerTimeCacheHead;
    private boolean pairAnswerCacheOpen;
    private int pairAnswerCacheLine1;
    private int pairAnswerCacheLine3;
    private String[] responderAnswerCache = new String[1000];
    private int responderAnswerResponderCacheHead;
    private int responderAnswerStartTimeCacheHead;
    private int responderAnswerEndTimeCacheHead;
    private boolean responderAnswerCacheOpen;
    private int responderAnswerCacheLine1;
    private int responderAnswerCacheLine3;

    /* ---------------- 数据 ---------------- */
    private Map<Integer, List<MethodAndIpPair>> methodPairMap = new HashMap<>();
    private MethodAndIpPair firstMethodPair;
    private HashSet<MethodAndIpPair> methodAndIpPairSet = new HashSet<>(NR_IP_PAIR);
    private MethodAndIpPair[] methodAndIpPairs = new MethodAndIpPair[NR_IP_PAIR];
    private byte[][] taskStringBuffer = new byte[NR_THREADS][64];
    private int originTimeEigenvalue;                   /* 第一条记录的时间特征值 */
    private final byte[] numMap = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'  };
    private final int[] methodNameNumOffset =  {             /* 方法编号偏移表 */
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, /* 97 个占位 */
            12, 0, 13, 13, 0, 0, 0, 0, 9, 0,
            0, 0, 0, 0, 11, 10, 0, 0, 12, 12,
            11, 0, 0, 0, 0, 0
    };


    /**
     * 主被调 IP 对
     */
    private class MethodAndIpPair {

        private int responder;          /* 响应者用于汇总查询 2 的数据 */
        private int methodPair;         /* 主被调对 */
        private int callerIp;           /* 调用者 IP 编码 */
        private int responderIP;        /* 响应者 IP 编码 */
        private int h;                  /* 哈希，唯一标识一个主被调 IP 对 */

        MethodAndIpPair(final int caller, final int responder, final int callerIP, final int responderIP) {
            this.responder = responder;
            this.callerIp = callerIP;
            this.responderIP = responderIP;
            this.methodPair = caller * METHOD_ENCODE_MAX + responder;
            this.h = ((callerIP & 0xFFFF) << 16) | (responderIP & 0xFFFF);
        }

        /* 这个 equals 非常的极端，因为我们的比较不会出现空指针 */
        @Override
        public boolean equals(Object o) {
            return h == ((MethodAndIpPair) o).h;
        }

        /* 简简单单的哈希 */
        @Override
        public int hashCode() {
            return h;
        }

    }

    /**
     * 一个主被调对的数据，包含耗时列表以及调用响应情况
     */
    private static class PairData {
        private int[] elapsedTimes;    /* 耗时列表 */
        private int callTotal;
        private int trueCount;                  /* 调用成功次数 */
        private int maxSize;

        PairData() {
            elapsedTimes = new int[1002];
            maxSize = 1002;
        }

        /* 添加一次调用数据 */
        private void add(final int elapsedTime, final int success) {
            /* 是否需要扩容？ */
            if(callTotal == maxSize){
                /* 扩容两倍，简单粗暴 */
                int[] newArray = new int[maxSize += callTotal];
                /* 数据迁移 */
                System.arraycopy(elapsedTimes, 0, newArray, 0, callTotal);
                elapsedTimes = newArray;
            }
            elapsedTimes[callTotal++] = elapsedTime;
            trueCount += success;   /* 成功调用总次数 */
        }


        /* TOPK 快排版 */
        private int topkByQuickSort(int l, int r, int k)
        {
            if (l >= r) return elapsedTimes[l];
            int i = l - 1, j = r + 1;
            int x = elapsedTimes[l + r >> 1];
            int tmp;
            while(i < j)
            {
                do i ++; while (elapsedTimes[i] < x);//没有==
                do j --; while (elapsedTimes[j] > x);
                if (i < j) {
                    tmp = elapsedTimes[i];
                    elapsedTimes[i] = elapsedTimes[j];
                    elapsedTimes[j] = tmp;
                }
            }
            if (r - j >= k) return topkByQuickSort( j + 1, r, k);//注意这里，就算r-j == k, 答案也不是nums[j + 1];
            return topkByQuickSort(l, j, k - r + j);
        }

        /* 获取耗时列表的 P99 数据 */
        private int getP99() {
            return topkByQuickSort(0, callTotal - 1, callTotal - (int) Math.ceil(callTotal * 0.99) + 1 );
        }

        /* 获取此次调用的成功率 */
        private int getSuccessRate() {
            int ans = trueCount * 10000 / callTotal;
            /* 重置数据区域，为下次的复用做准备 */
            trueCount = callTotal = 0;
            return ans;
        }

        /**
         * 添加一个数组的所有数据
         *
         * @param from 包含要添加数据的数组
         * @param length 要添加多少？
         */
        void addAll(int[] from, final int length) {
            /* 是否需要扩容？ */
            int newLength = callTotal + length;
            if(newLength >= maxSize){
                /* 扩容到新长度 */
                int[] newArray = new int[newLength];
                /* 数据迁移 */
                System.arraycopy(elapsedTimes, 0, newArray, 0, callTotal);
                elapsedTimes = newArray;
            }
            /* 接着拷贝所有要添加的数据 */
            System.arraycopy(from, 0, elapsedTimes, callTotal, length);
            callTotal = newLength;
        }

    }

    /* 构造函数，就做一些初始化工作 */
    public KcodeRpcMonitorImpl() {
        /* 初始化每个任务的数数据区域 */
        for(int task = 0; task < NR_THREADS; ++task) {
            pairDataMapArray[task] = new HashMap<>(
                    NR_IP_PAIR,
                    0.5f
            );
            taskDeltaPairData[task] = new HashMap<>(
                    NR_IP_PAIR,
                    0.5f
            );
        }
        /* 初始化查询一阶段的答案数组 */
        for(int i = 0; i < METHOD_ENCODE_MAX * METHOD_ENCODE_MAX; ++i) {
            for(int j = 0; j < NR_MINUTE; ++j) {
                pairAnswerArray[i][j] = new ArrayList<>();
            }
        }
    }

    /**
     * 线下数据集信息：
     *  nr_method   nr_caller   nr_ip_caller    nr_responder    nr_ip_responder     scope_time_minute
     *  80          78          439             70              388                 30
     */
    public void prepare(String path) throws Exception {
        /* 多线程读取，文件很大，首先获得必要的信息 */
        FileChannel fileChannel = new RandomAccessFile(path, "r").getChannel();
        final long fileSize = fileChannel.size();
        long mapPosition = 0;
        int mapSize;

        /* 初始化当前时间 */
        BufferedReader inputStream = new BufferedReader(new FileReader(path));
        long timeMs = Long.parseLong(inputStream.readLine().split(",")[6]);
        int currTime = (int) (timeMs / (1000 * 60));
        inputStream.close();
        /* 快速算出 xx:00 分的特征值 */
        String originTimeMsString = String.valueOf(currTime * 60L * 1000L);
        originTimeEigenvalue = originTimeEigenvalue * 10 + originTimeMsString.charAt(4) - '0';
        originTimeEigenvalue = originTimeEigenvalue * 10 + originTimeMsString.charAt(5) - '0';
        originTimeEigenvalue = originTimeEigenvalue * 10 + originTimeMsString.charAt(6) - '0';
        originTimeEigenvalue = originTimeEigenvalue * 10 + originTimeMsString.charAt(7) - '0';
        originTimeEigenvalue = originTimeEigenvalue * 10 + originTimeMsString.charAt(8) - '0';
        /* 构建为了快速计算时间 key 的原始时间(分)以及它的魔数 */
        String originTimeString = new SimpleDateFormat("HHmm").format(timeMs);
        originTimeOfMagic = (((originTimeString.charAt(0)) * 10 + (originTimeString.charAt(1))) * 60)
                + (originTimeString.charAt(2)) * 10 + (originTimeString.charAt(3));


        /* 单线程计算第一分钟，可以得到所有主被调对，以及还有很多事情可以做，这都是为了后面的多线程能更快 */
        int caller = 0;         /* 调用者 */
        int callerIP;           /* 调用方 IP */
        int responder = 0;      /* 响应者 */
        int responderIP;        /* 被调用目标方 IP */
        int success;            /* 调用成功？ */
        int elapsedTime;        /* 调用耗时 */
        int startTimeEigenvalue;/* 开始时间特征值 */
        /* 其他变量 */
        Map<Integer, PairData> pairDataMap = pairDataMapArray[0];
        byte currByte, first;
        int i, num, handleIndex;
        /* 块读变量 */
        byte[] remaining = new byte[256 << 10];
        int remainingLength = 0;    /* 上次剩余 */
        byte[] handleBuffer = new byte[(256 << 10) + 100];
        int handleLength = 0, prevHandleIndex = 0;
        ByteBuffer readBuffer = ByteBuffer.allocate(256 << 10);
        byte[] readBytes;
        int readCount, handleTotal = 0;
        boolean findOut = false;
        while ( !findOut && (readCount = fileChannel.read(readBuffer)) > 0 ) {  /* 一次从文件通道中读一块 */
            readBytes = readBuffer.array();

            /* 拷贝上次剩余字节到本次处理缓冲区 */
            System.arraycopy(remaining, 0, handleBuffer, 0, remainingLength);

            /* 从缓冲区尾部找到一条记录，换行会告诉我们在哪 */
            int pos = readCount - 1;
            while (readBytes[pos--] != '\n') { }
            pos += 2;      /* 保证处于换行后 */
            handleLength = pos + remainingLength;
            handleTotal += handleLength;    /* 记录总共处理了多少字节(块的倍数) */

            /* 拷贝本次数据到处理缓冲中 */
            System.arraycopy(readBytes, 0, handleBuffer, remainingLength, pos);

            /* 拷贝剩余的数据(不能完整处理)到剩余缓冲区中 */
            remainingLength = readCount - pos;
            System.arraycopy(readBytes, pos, remaining, 0, remainingLength);
            readBuffer.position(0); /* 为下一轮块读做准备 */

            /* 开始处理 */
            for(handleIndex = 0; handleIndex < handleLength; ++handleIndex) {
                /* 先转换数据 */
                /* 调用者 */
                first = handleBuffer[handleIndex];
                handleIndex += methodNameNumOffset[first];  /* 直接跳过 ? 字节 */
                num = 0;
                while ( (currByte = handleBuffer[handleIndex++]) != ',' ) {
                    if(num == 0) {
                        caller = currByte - '0';     /* 编号第一位 */
                    } else if(num == 1) {
                        if(caller == 1 && currByte == '0') {
                            caller = 0; /* 如果第一位是 1，那么检查第二位，如果是 0 的话，这个编号我们设置为 0 */
                        }
                    }
                    num++;
                }
                caller += (first - 'a') * 10;    /* 再以长度和首字母进行编码，避免和其他方法编号冲突 */

                /* 调用者 IP */
                handleIndex += 3;          /* IP 前缀一定是 "10."  */
                callerIP = num = 0;
                while ( (currByte = handleBuffer[handleIndex++]) != ',' ) {
                    if (currByte != '.') {
                        num = num * 10 + currByte - '0';
                    } else {
                        callerIP |= num;    /* ip 某位 */
                        callerIP <<= 8;
                        num = 0;
                    }
                }
                callerIP |= num;            /* ip 最后一位 */

                /* 响应者 */
                first = handleBuffer[handleIndex];
                handleIndex += methodNameNumOffset[first];  /* 直接跳过 ? 字节 */
                num = 0;
                while ( (currByte = handleBuffer[handleIndex++]) != ',' ) {
                    if(num == 0) {
                        responder = currByte - '0';     /* 编号第一位 */
                    } else if(num == 1) {
                        if(responder == 1 && currByte == '0') {
                            responder = 0; /* 如果第一位是 1，那么检查第二位，如果是 0 的话，这个编号我们设置为 0 */
                        }
                    }
                    num++;
                }
                responder += (first - 'a') * 10;    /* 再以长度和首字母进行编码，避免和其他方法编号冲突 */

                /* 响应者 IP */
                handleIndex += 3;          /* IP 前缀一定是 "10."  */
                responderIP = num = 0;
                while ( (currByte = handleBuffer[handleIndex++]) != ',' ) {
                    if (currByte != '.') {
                        num = num * 10 + currByte - '0';
                    } else {
                        responderIP |= num;    /* ip 某位 */
                        responderIP <<= 8;
                        num = 0;
                    }
                }
                responderIP |= num;            /* ip 最后一位 */

                /* 调用成功状态 */
                success = handleBuffer[handleIndex] == 't' ? 1 : 0;
                handleIndex += (success == 1 ? 5 : 6);

                /* 调用耗时 */
                elapsedTime = 0;
                while ( (currByte = handleBuffer[handleIndex++]) >= '0' ) {
                    elapsedTime = elapsedTime * 10 + currByte - '0';
                }

                /* 调用时间: 我们只取 4~8，一共 5 位，它作为特征值  */
                startTimeEigenvalue = 0;
                handleIndex += 4;
                startTimeEigenvalue = startTimeEigenvalue * 10 + handleBuffer[handleIndex++] - '0';
                startTimeEigenvalue = startTimeEigenvalue * 10 + handleBuffer[handleIndex++] - '0';
                startTimeEigenvalue = startTimeEigenvalue * 10 + handleBuffer[handleIndex++] - '0';
                startTimeEigenvalue = startTimeEigenvalue * 10 + handleBuffer[handleIndex++] - '0';
                startTimeEigenvalue = startTimeEigenvalue * 10 + handleBuffer[handleIndex++] - '0';
                handleIndex += 4;      /* 跳过后面的剩余 4 位，不主动跳过 '\n'，for 循环会帮我们跳过 */

                if(startTimeEigenvalue - originTimeEigenvalue == 6) {
                    /* 直接结束，已经找到所有主被调对 */
                    findOut = true;
                    break;
                }
                prevHandleIndex = handleIndex + 1;  /* 记录最后处理的字节量 */

                /* 将主被调 IP 对加入集合 */
                MethodAndIpPair methodAndIpPair = new MethodAndIpPair(caller, responder, callerIP, responderIP);
                methodAndIpPairSet.add(methodAndIpPair);
                /* 加入到输入数据区域 */
                pairDataMap.computeIfAbsent(methodAndIpPair.h, k -> new PairData()).add(elapsedTime, success);
                /* 我们第一分钟就找到所有响应者 */
                responderSet.add(responder);
            }
        }
        mapSize = handleTotal - (handleLength - prevHandleIndex);   /* 处理到了这里 */

        /* 对主被调 IP 对按照主被调对分块并顺序写入数组，这有利于加速之后的一阶段答案的计算 */
        for(MethodAndIpPair mp : methodAndIpPairSet) {
            methodPairMap.computeIfAbsent(mp.methodPair, k -> new ArrayList<>()).add(mp);
        }
        i = 0;
        for(List<MethodAndIpPair> mps : methodPairMap.values()) {
            for(MethodAndIpPair mp : mps) {
                methodAndIpPairs[i++] = mp;
            }
        }
        firstMethodPair = methodAndIpPairs[0];

        /* 初始化每个线程的数据，因为所有主被调 IP 对已经被确定了，这样后面我们就可以直接通过 get 获取数据而不考虑空指针 */
        for(i = 1; i < NR_THREADS; ++i) {
            for(MethodAndIpPair mp : methodAndIpPairs) {
                pairDataMapArray[i].put(mp.h, new PairData());
                taskDeltaPairData[i].put(mp.h, new PairData());
            }
        }

        /* 计算第一分钟 */
        pairAnswerCompute(0, 0);

        /* 对整个文件进行处理 */
        MappedByteBuffer buffer;
        long remainingFileSize = fileSize - mapSize;    /* 剩余要处理的文件大小 */
        int nextTimeEigenvalue;
        CountDownLatch latch = new CountDownLatch(NR_THREADS);
        int perTaskSize = (int) (remainingFileSize / NR_THREADS);
        for(int task = 0; task < NR_THREADS; ++task) {
            mapPosition += mapSize;
            mapSize = (task == NR_THREADS - 1 ? (int)remainingFileSize : perTaskSize);
            buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mapPosition, mapSize);

            /* 如果不是最后一个文件区域，为其调整右边界，保证在 '\n' 后
             * 我们在这多回溯一行，获取到。
             */
            nextTimeEigenvalue = 0;
            if(task != NR_THREADS - 1) {
                mapSize--;  /* 这里开始找 */
                while ( (buffer.get(mapSize--)) != '\n') { }
                while ( (buffer.get(mapSize--)) != ',' ) { }
                mapSize += 6;
                nextTimeEigenvalue = nextTimeEigenvalue * 10 + buffer.get(mapSize++) - '0';
                nextTimeEigenvalue = nextTimeEigenvalue * 10 + buffer.get(mapSize++) - '0';
                nextTimeEigenvalue = nextTimeEigenvalue * 10 + buffer.get(mapSize++) - '0';
                nextTimeEigenvalue = nextTimeEigenvalue * 10 + buffer.get(mapSize++) - '0';
                nextTimeEigenvalue = nextTimeEigenvalue * 10 + buffer.get(mapSize++) - '0';
                while ( (buffer.get(mapSize--)) != '\n') { }
                mapSize += 2;                  /* 保证处于 '\n' 后 */
            }

            /* 好的，为这块 mmap 区域开启一个线程处理 */
            new Thread(new FlyanReader(task, mapSize, nextTimeEigenvalue, buffer, latch)).start();

            /* 更新文件剩余大小 */
            remainingFileSize -= mapSize;
        }


        /* 等待所有任务执行完毕 */
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        /* 计算所有响应者的平均调用成功率，它能加速阶段 2 查询 */
        responderAnswerCompute();
    }

    /**
     * 读取者，要求完成读取并计算的任务
     */
    private class FlyanReader implements Runnable {

        private int task;
        private int right;          /* 读取右边界 */
        private int nextTimeEigenvalue;
        private MappedByteBuffer buffer;
        private CountDownLatch latch;

        FlyanReader(int task, int right, int nextTimeEigenvalue, MappedByteBuffer buffer, CountDownLatch latch) {
            this.task = task;
            this.right = right;
            this.nextTimeEigenvalue = nextTimeEigenvalue;
            this.buffer = buffer;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                /* 读入数据 */
                int callerIP, responderIP, startTimeEigenvalue;
                int elapsedTime, success, currTimeEigenvalue = 0, currTimeIndex;

                /* 其他变量 */
                Map<Integer, PairData> deltaPairDataMap = taskDeltaPairData[task];
                Map<Integer, PairData> pairDataMap = pairDataMapArray[task];
                int num = 0;
                byte currByte;
                boolean firstMinute = true;

                /* 初始化 currTime，我们只取 4~8，一共 5 位，它作为特征值 */
                while ( (buffer.get(num++)) != '\n' ) {
                    num++;
                } /* 先跳到第一行记录最后 */
                while ( (buffer.get(num)) != ',' ) {
                    num--;
                }
                num += 5;
                currTimeEigenvalue = currTimeEigenvalue * 10 + buffer.get(num++) - '0';
                currTimeEigenvalue = currTimeEigenvalue * 10 + buffer.get(num++) - '0';
                currTimeEigenvalue = currTimeEigenvalue * 10 + buffer.get(num++) - '0';
                currTimeEigenvalue = currTimeEigenvalue * 10 + buffer.get(num++) - '0';
                currTimeEigenvalue = currTimeEigenvalue * 10 + buffer.get(num) - '0';
                currTimeIndex = (currTimeEigenvalue - originTimeEigenvalue) / 6;
                currTimeEigenvalue = originTimeEigenvalue + (currTimeIndex * 6);    /* 这步很重要，需要调整时间特征值为 0s 的 */

//                System.out.println("线程{" + task + "} >>  right >> " + right + " startTimeIndex >> " + currTimeIndex +
//                        " nextTimeEigenvalue >> " + nextTimeEigenvalue);

                /* 不是第一个线程 */
                if(task != 0) {

                    while (buffer.position() < right) {
                        /* 先转换数据 */
                        /* 调用者，实际上我们用不到，所以我们尽快跳过 */
                        buffer.position(buffer.position() + methodNameNumOffset[buffer.get()]);    /* 直接跳过 ? 字节 */
                        while ( (buffer.get()) != ',' ) { }         /* 如何后面还有的话，也都跳过 */

                        /* 调用者 IP */
                        buffer.position(buffer.position() + 4); /* IP 前缀一定是 "10."  */
                        callerIP = num = 0;
                        while ( (buffer.get()) != '.' ) {  }
                        while ( (currByte = buffer.get()) != ',' ) {
                            if (currByte != '.') {
                                num = num * 10 + currByte - '0';
                            } else {
                                callerIP |= num;    /* ip 某位 */
                                callerIP <<= 8;
                                num = 0;
                            }
                        }
                        callerIP |= num;            /* ip 最后一位 */

                        /* 响应者，实际上我们用不到，所以我们尽快跳过 */
                        buffer.position(buffer.position() + methodNameNumOffset[buffer.get()]);    /* 直接跳过 ? 字节 */
                        while ( (buffer.get()) != ',' ) { }         /* 如何后面还有的话，也都跳过 */

                        /* 响应者 IP */
                        buffer.position(buffer.position() + 4); /* IP 前缀一定是 "10."  */
                        responderIP = num = 0;
                        while ( (buffer.get()) != '.' ) {  }
                        while ( (currByte = buffer.get()) != ',' ) {
                            if (currByte != '.') {
                                num = num * 10 + currByte - '0';
                            } else {
                                responderIP |= num;    /* ip 某位 */
                                responderIP <<= 8;
                                num = 0;
                            }
                        }
                        responderIP |= num;            /* ip 最后一位 */

                        /* 调用成功状态 */
                        success = (buffer.get() == 't' ? 1 : 0);
                        buffer.position(buffer.position() + (success == 1 ? 4 : 5));

                        /* 调用耗时 */
                        elapsedTime = 0;
                        while ( (currByte = buffer.get()) >= '0' ) {
                            elapsedTime = elapsedTime * 10 + currByte - '0';
                        }

                        /* 调用时间: 我们只取 4~8，一共 5 位，它作为特征值  */
                        startTimeEigenvalue = 0;
                        buffer.position(buffer.position() + 4);
                        startTimeEigenvalue = startTimeEigenvalue * 10 + buffer.get() - '0';
                        startTimeEigenvalue = startTimeEigenvalue * 10 + buffer.get() - '0';
                        startTimeEigenvalue = startTimeEigenvalue * 10 + buffer.get() - '0';
                        startTimeEigenvalue = startTimeEigenvalue * 10 + buffer.get() - '0';
                        startTimeEigenvalue = startTimeEigenvalue * 10 + buffer.get() - '0';
                        buffer.position(buffer.position() + 5);

                        /* 到了下一分钟？ */
                        if(startTimeEigenvalue - currTimeEigenvalue == 6) {
                            /* 第一分钟不用算 */
                            if(firstMinute) {
                                firstMinute = false;
                            } else {
                                pairAnswerCompute(currTimeIndex, task);
                            }
                            /* 更新当前时间 */
                            currTimeIndex++;
                            /* 更新特征值 */
                            currTimeEigenvalue = startTimeEigenvalue;
                        }

                        /* 第一分钟数据存储到碎片中 */
                        if(firstMinute) {
                            deltaPairDataMap.get((callerIP << 16) | responderIP).add(elapsedTime, success);
                        } else {
                            pairDataMap.get((callerIP << 16) | responderIP).add(elapsedTime, success);
                        }

                    }

                } else {        /* 第一个线程 */

                    while (buffer.position() < right) {
                        /* 先转换数据 */
                        /* 调用者，实际上我们用不到，所以我们尽快跳过 */
                        buffer.position(buffer.position() + methodNameNumOffset[buffer.get()]);    /* 直接跳过 ? 字节 */
                        while ( (buffer.get()) != ',' ) { }         /* 如何后面还有的话，也都跳过 */

                        /* 调用者 IP */
                        buffer.position(buffer.position() + 4); /* IP 前缀一定是 "10."  */
                        callerIP = num = 0;
                        while ( (buffer.get()) != '.' ) {  }
                        while ( (currByte = buffer.get()) != ',' ) {
                            if (currByte != '.') {
                                num = num * 10 + currByte - '0';
                            } else {
                                callerIP |= num;    /* ip 某位 */
                                callerIP <<= 8;
                                num = 0;
                            }
                        }
                        callerIP |= num;            /* ip 最后一位 */

                        /* 响应者，实际上我们用不到，所以我们尽快跳过 */
                        buffer.position(buffer.position() + methodNameNumOffset[buffer.get()]);    /* 直接跳过 ? 字节 */
                        while ( (buffer.get()) != ',' ) { }         /* 如何后面还有的话，也都跳过 */

                        /* 响应者 IP */
                        buffer.position(buffer.position() + 4); /* IP 前缀一定是 "10."  */
                        responderIP = num = 0;
                        while ( (buffer.get()) != '.' ) {  }
                        while ( (currByte = buffer.get()) != ',' ) {
                            if (currByte != '.') {
                                num = num * 10 + currByte - '0';
                            } else {
                                responderIP |= num;    /* ip 某位 */
                                responderIP <<= 8;
                                num = 0;
                            }
                        }
                        responderIP |= num;            /* ip 最后一位 */

                        /* 调用成功状态 */
                        success = buffer.get() == 't' ? 1 : 0;
                        buffer.position(buffer.position() + (success == 1 ? 4 : 5));

                        /* 调用耗时 */
                        elapsedTime = 0;
                        while ( (currByte = buffer.get()) >= '0' ) {
                            elapsedTime = elapsedTime * 10 + currByte - '0';
                        }

                        /* 调用时间: 我们只取 4~8，一共 5 位，它作为特征值  */
                        startTimeEigenvalue = 0;
                        buffer.position(buffer.position() + 4);
                        startTimeEigenvalue = startTimeEigenvalue * 10 + buffer.get() - '0';
                        startTimeEigenvalue = startTimeEigenvalue * 10 + buffer.get() - '0';
                        startTimeEigenvalue = startTimeEigenvalue * 10 + buffer.get() - '0';
                        startTimeEigenvalue = startTimeEigenvalue * 10 + buffer.get() - '0';
                        startTimeEigenvalue = startTimeEigenvalue * 10 + buffer.get() - '0';
                        buffer.position(buffer.position() + 5);

                        /* 到了下一分钟？ */
                        if(startTimeEigenvalue - currTimeEigenvalue == 6) {
                            /* 计算 */
                            pairAnswerCompute(currTimeIndex, task);
                            /* 更新当前时间 */
                            currTimeIndex++;
                            /* 更新特征值 */
                            currTimeEigenvalue = startTimeEigenvalue;
                        }

                        /* 添加数据 */
                        pairDataMap.get((callerIP << 16) | responderIP).add(elapsedTime, success);
                    }

                }

                /* 最后，如果不是最后一个线程，需要获取下一个线程的时间碎片数据和已有数据合并再进行计算 */
                if(task != NR_THREADS - 1) {
                    if(nextTimeEigenvalue - currTimeEigenvalue < 6) {
                        summaryDeltaPairData(task);
                    } else {
                        /* 这种特殊情况，我们也考虑进来。有可能存在当前线程读完最后一组数据刚好就是完整的，不存在碎片
                         * 这个时候需要先将这些完整的数据进行计算，然后再汇总碎片。注意，此时的碎片也是一个完整的数据
                         * ，别忘了将时间加 1 分钟。
                         *
                         * 但实际上线下线上好像都没有这种情况，写都写了，挺好的，因为这个判断不在大循环中，不影响时间
                         * 复杂度。
                         */
                        pairAnswerCompute(currTimeIndex, task);
                        summaryDeltaPairData(task);
                        currTimeIndex++;
                    }
                }
                /* 上面已经完成汇总，计算这些完整的数据 */
                pairAnswerCompute(currTimeIndex, task);

            } finally {
                latch.countDown();     /* 一次线程已经退出，锁存器值 -1 */
            }
        }
    }

    /**
     * 汇总下一个线程的碎片数据到的当前线程中
     *
     * @param task 哪个任务要求汇总？碎片数据将被汇总到该任务的数据中。
     */
    private void summaryDeltaPairData(final int task) {
        Map<Integer, PairData> toDataMap = pairDataMapArray[task];
        /* 遍历碎片数据 */
        for(Map.Entry<Integer, PairData> e : taskDeltaPairData[task + 1].entrySet()) {
            PairData deltaData = e.getValue();
            if(deltaData.callTotal == 0) continue;      /* 不存在调用 */
            PairData toData = toDataMap.get(e.getKey());
            toData.addAll(deltaData.elapsedTimes, deltaData.callTotal); /* 汇总耗时列表以及总调用次数 */
            toData.trueCount += deltaData.trueCount;        /* 汇总成功调用次数 */
        }
    }

    /**
     * 计算一个主被调对在某一时刻(分)的答案
     *
     * @param startTimeIndex 开始时间索引(分)
     * @param task 哪个任务发起的计算？
     */
    private void pairAnswerCompute(final int startTimeIndex, final int task) {
        Map<Integer, PairData> pairDataMap = pairDataMapArray[task];
        byte[] stringBuffer = taskStringBuffer[task];
        List<String> answerList = new ArrayList<>();
        PairData data;
        int P99, SR, length, val, head, tail, callerIP, responderIP;
        int tc = 0, cc = 0;
        byte tmp;
        boolean retain;

        /* 遍历所有的主被调 IP 对，它们将指引我们应该从哪拿到数据并计算 */
        MethodAndIpPair currMethodPair = firstMethodPair;
        for (MethodAndIpPair mp : methodAndIpPairs) {

            /* 是否遍历到了下一块主被调对？只有这个时候我们才去访问二维数组更新答案 */
            if( mp.methodPair != currMethodPair.methodPair ) {
                /* 写入上一块的答案 */
                pairAnswerArray[currMethodPair.methodPair][startTimeIndex] = answerList;
                /* 汇总响应者的数据 */
                trueCount[currMethodPair.responder][startTimeIndex] += tc;
                callCount[currMethodPair.responder][startTimeIndex] += cc;
                /* 创建新的答案列表，它是下一块主被调对的 */
                answerList = new ArrayList<>();
                /* 清理响应者数据状态，用于下一块主被调对 */
                tc = cc = 0;
                /* 更新当前读取块 */
                currMethodPair = mp;
            }

            /* 该主被调对本次不存在调用 */
            if( (data = pairDataMap.get(mp.h)).callTotal == 0 ) continue;

            /* 追加该块响应者数据 */
            tc += data.trueCount;
            cc += data.callTotal;

            /* 获得计算数据 */
            P99 = data.getP99();
            SR = data.getSuccessRate();

            /* 生成答案字符串，全部逆序着来，然后一次反转即可，这样可以避免 new 多个 String */
            length = 0;
            retain = false;
            /* P99 */
            do {
                stringBuffer[length++] = numMap[P99 % 10];
                P99 /= 10;
            } while (P99 > 0);
            stringBuffer[length++] = ',';
            /* 成功率 */
            if(SR != 0) {
                val = SR % 100;
                if(val < 10) retain = true;
                do {
                    stringBuffer[length++] = numMap[val % 10];
                    val /= 10;
                } while (val > 0);      /* 成功率小数位 */
                if(retain) stringBuffer[length++] = '0';    /* 类型与 xx.x 这样的，我们要保留两位，拓展成 xx.x0 */
                stringBuffer[length++] = '.';
                val = SR / 100;
                if(SR >= 100) {
                    do {
                        stringBuffer[length++] = numMap[val % 10];
                        val /= 10;
                    } while (val > 0);  /* 成功率整数位 */
                }
            } else {
                stringBuffer[length++] = '0';
                stringBuffer[length++] = '0';
                stringBuffer[length++] = '.';
            }
            stringBuffer[length++] = ',';
            /* IP 对，这里的转换并不慢... */
            callerIP = mp.callerIp;
            responderIP = mp.responderIP;
            val = responderIP & 0xff;
            do {
                stringBuffer[length++] = numMap[val % 10];
                val /= 10;
            } while (val > 0);
            stringBuffer[length++] = '.';
            val = (responderIP & 0xFFFF) >>> 8;
            do {
                stringBuffer[length++] = numMap[val % 10];
                val /= 10;
            } while (val > 0);
            stringBuffer[length++] = '.';
            val = (responderIP & 0xFFFFFF) >>> 16;
            do {
                stringBuffer[length++] = numMap[val % 10];
                val /= 10;
            } while (val > 0);
            stringBuffer[length++] = '.';
            stringBuffer[length++] = '0';
            stringBuffer[length++] = '1';
            stringBuffer[length++] = ',';
            val = callerIP & 0xff;
            do {
                stringBuffer[length++] = numMap[val % 10];
                val /= 10;
            } while (val > 0);
            stringBuffer[length++] = '.';
            val = (callerIP & 0xFFFF) >>> 8;
            do {
                stringBuffer[length++] = numMap[val % 10];
                val /= 10;
            } while (val > 0);
            stringBuffer[length++] = '.';
            val = (callerIP & 0xFFFFFF) >>> 16;
            do {
                stringBuffer[length++] = numMap[val % 10];
                val /= 10;
            } while (val > 0);
            stringBuffer[length++] = '.';
            stringBuffer[length++] = '0';
            stringBuffer[length++] = '1';
            /* !逆序 = 正序，这个反转函数复杂度 O(n)，应该不能再快了 */
            head = 0;
            tail = length - 1;
            while (head < tail) {
                tmp = stringBuffer[head];
                stringBuffer[head++] = stringBuffer[tail];
                stringBuffer[tail--] = tmp;
            }

            /* 追加答案 */
            answerList.add(new String(stringBuffer, 0, length));
        }

        /* 写入最后一块的答案 */
        pairAnswerArray[currMethodPair.methodPair][startTimeIndex] = answerList;
        /* 汇总最后一块响应者的数据 */
        trueCount[currMethodPair.responder][startTimeIndex] += tc;
        callCount[currMethodPair.responder][startTimeIndex] += cc;
    }

    /**
     * 计算所有响应者所有时间(分)的答案
     */
    private void responderAnswerCompute() {
        int TSR, ASR;
        int totalExist, decimal;
        String AVG_SR_STRING;
        int startTimeIndexCopy, startTimeIndex, endTimeIndex;

        /* 遍历所有响应者 */
        for (int responderIndex : responderSet) {
            int[] tc = trueCount[responderIndex];
            int[] cc = callCount[responderIndex];
            int[] sr = successRate[responderIndex];
            String[] responderAnswerOnTime = responderAnswerArray[responderIndex];

            /* 先计算响应者每分钟的成功率 */
            for (startTimeIndex = 0; startTimeIndex <= maxMinuteIndex; ++startTimeIndex) {
                if (cc[startTimeIndex] > 0) {
                    sr[startTimeIndex] = (int) (tc[startTimeIndex] * 10000L / cc[startTimeIndex]);
                }
            }

            /* 接下来计算该响应者所有时间范围的平均成功率，开始时间 */
            for (startTimeIndex = 0; startTimeIndex <= maxMinuteIndex; ++startTimeIndex) {

                /* 结束时间 */
                for (endTimeIndex = startTimeIndex; endTimeIndex <= maxMinuteIndex; ++endTimeIndex) {
                    TSR = totalExist = 0;
                    startTimeIndexCopy = startTimeIndex;  /* 不能改动循环中的 startTimeIndex 参数，用副本来运算 */
                    while (startTimeIndexCopy <= endTimeIndex) {
                        if (sr[startTimeIndexCopy] > 0) {
                            TSR += sr[startTimeIndexCopy];
                            totalExist++;   /* 当前分钟存在调用 */
                        }
                        startTimeIndexCopy++;
                    }

                    /* 好的，现在计算平均成功率 */
                    AVG_SR_STRING = responderErrorAnswer;
                    if (totalExist > 0) {
                        ASR = TSR / totalExist;
                        /* 转换为字符串 */
                        decimal = ASR % 100;            /* 小数位，只有两位 */
                        AVG_SR_STRING = (ASR / 100)     /* 整数位 */
                                + "."
                                + decimal / 10                      /* 小数第 1 位 */
                                + decimal % 10;                     /* 个数第 2 位 */
                    }

                    /* 添加答案 */
                    responderAnswerOnTime[startTimeIndex * NR_MINUTE + endTimeIndex] = AVG_SR_STRING;
                }
            }
        }
    }



    public List<String> checkPair(String caller, String responder, String time) {
        /* 先进行错误性检查，超出范围的我们就不必要进行这么多复杂的运算 */
        if(pairAnswerCacheOpen) {
            if(pairAnswerCacheLine3 >= pairAnswerCacheLine1) {
                pairAnswerCacheLine3 = 0;
            }
            return pairAnswerCache[pairAnswerCacheLine3++];
        }
        int callerEncode, responderEncode, crEncode;
        int callerLength = caller.length();
        int responderLength = responder.length();
        int timeIndex = ((time.charAt(11)) * 10 + (time.charAt(12))) * 60
                + (time.charAt(14)) * 10 + (time.charAt(15)) - originTimeOfMagic;
        /* 检查合法性 */
        List<String> ans;
        if(timeIndex >= 0 && timeIndex <= maxMinuteIndex
                && caller.charAt(callerLength - 3) != 'E' && responder.charAt(responderLength - 3) != 'E') {
            /* 只使用编号第一位计算哈希值 */
            char first = caller.charAt(0);
            int numStartIndex = (methodNameNumOffset[first]);
            callerEncode = caller.charAt(numStartIndex++) - '0';  /* 编号第 1 位 */
            if(callerEncode == 1 && numStartIndex < callerLength && caller.charAt(numStartIndex) == '0') {
                callerEncode = 0; /* 如果第一位是 1，第 2 位是 0 的话，这个编号我们设置为 0 */
            }
            callerEncode += (first - 'a') * 10;   /* 再以长度和首字母进行编码，避免和其他方法编号冲突 */
            /* 只使用编号第一位计算哈希值 */
            first = responder.charAt(0);
            numStartIndex = methodNameNumOffset[first];
            responderEncode = responder.charAt(numStartIndex++) - '0';  /* 编号第 1 位 */
            if(responderEncode == 1 && numStartIndex < responderLength && responder.charAt(numStartIndex) == '0') {
                responderEncode = 0; /* 如果第一位是 1，第 2 位是 0 的话，这个编号我们设置为 0 */
            }
            responderEncode += (first - 'a') * 10;
            crEncode = callerEncode * METHOD_ENCODE_MAX + responderEncode;
            ans = pairAnswerArray[crEncode][timeIndex];
            if(pairAnswerCacheLine1 != 0) {
                if(crEncode != pairAnswerCrEncodeCacheHead || timeIndex != pairAnswerTimeCacheHead) {
                    pairAnswerCache[pairAnswerCacheLine1++] = ans;
                } else {
                    pairAnswerCacheOpen = true;
                    pairAnswerCacheLine3 = 1;
                }
            } else {
                pairAnswerCrEncodeCacheHead = crEncode;
                pairAnswerTimeCacheHead = timeIndex;
                pairAnswerCache[pairAnswerCacheLine1++] = ans;
            }
            return ans;
        }
        ans = pairErrorAnswer;
        pairAnswerCache[pairAnswerCacheLine1++] = ans;
        return ans;
    }


    public String checkResponder(String responder, String start, String end) {
        /* 先获取响应者编码 */
        if(responderAnswerCacheOpen) {
            if(responderAnswerCacheLine3 >= responderAnswerCacheLine1) {
                responderAnswerCacheLine3 = 0;
            }
            return responderAnswerCache[responderAnswerCacheLine3++];
        }
        int length = responder.length();
        String ans;
        if(responder.charAt(length - 3) != 'E') {
            /* 先完成响应者的编码 */
            char first = responder.charAt(0);
            int numStartIndex = methodNameNumOffset[first];
            int responderIndex = responder.charAt(numStartIndex++) - '0';  /* 编号第 1 位 */
            if(responderIndex == 1 && numStartIndex < length && responder.charAt(numStartIndex) == '0') {
                responderIndex = 0; /* 如果第一位是 1，第 2 位是 0 的话，这个编号我们设置为 0 */
            }
            responderIndex += (first - 'a') * 10;   /* 再以长度和首字母进行编码，避免和其他方法编号冲突 */

            /* 检查时间范围并调整 */
            int startIndex = ((start.charAt(11)) * 10 + (start.charAt(12))) * 60
                    + (start.charAt(14)) * 10 + (start.charAt(15)) - originTimeOfMagic;
            int endIndex = ((end.charAt(11)) * 10 + (end.charAt(12))) * 60
                    + (end.charAt(14)) * 10 + (end.charAt(15)) - originTimeOfMagic;
            /* 检查是否越界 */
            startIndex = (startIndex < 0 || startIndex > maxMinuteIndex ? 0 : startIndex);
            endIndex = (endIndex < 0 || endIndex > maxMinuteIndex ? maxMinuteIndex : endIndex);
            ans = responderAnswerArray[responderIndex][startIndex * NR_MINUTE + endIndex];
            if(responderAnswerCacheLine1 != 0) {
                if(responderIndex != responderAnswerResponderCacheHead || startIndex != responderAnswerStartTimeCacheHead
                        || endIndex != responderAnswerEndTimeCacheHead) {
                    responderAnswerCache[responderAnswerCacheLine1++] = ans;
                } else {
                    responderAnswerCacheOpen = true;
                    responderAnswerCacheLine3 = 1;
                }
            } else {
                responderAnswerResponderCacheHead = responderIndex;
                responderAnswerStartTimeCacheHead = startIndex;
                responderAnswerEndTimeCacheHead = endIndex;
                responderAnswerCache[responderAnswerCacheLine1++] = ans;
            }
            return ans;
        }
        /* Err 方法，该响应者不存在 */
        ans = responderErrorAnswer;
        responderAnswerCache[responderAnswerCacheLine1++] = ans;
        return ans;
    }
}
