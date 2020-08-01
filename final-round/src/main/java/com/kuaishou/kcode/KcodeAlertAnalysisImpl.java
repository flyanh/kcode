package com.kuaishou.kcode;

import com.kuaishou.kcode.domain.*;
import com.kuaishou.kcode.domain.fuc.ComputingAccessory;
import com.kuaishou.kcode.domain.fuc.ParsingAccessory;
import com.kuaishou.kcode.domain.kv.Q2Key;
import com.kuaishou.kcode.domain.kv.ServiceIpPair;
import com.kuaishou.kcode.domain.kv.ServiceIpPairAnswer;
import com.kuaishou.kcode.domain.kv.ServicePair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @team 辰海飞燕
 * @member 陈元崇 - 济南大学泉城学院
 * @member 张浩宇 - 哈尔滨工业大学
 * @member 王俊杰 - 南京理工大学
 *
 * Created on 2020-07-06
 *
 * @paper 7-06 baseline 使用单线程块读完成(ok)
 * @paper 7-08 建图使用链式前向星(ok)
 * @paper 7-09 去除所有硬编码，为了鲁棒性(ok)
 * @paper 7-10 第二阶段的数据应该在第一阶段答案计算的时候进行汇总，减少读取时的工作量(ok 单线程提升了 5s 第一个数据集)
 * @paper 7-10 提前计算阶段 2 的答案(ok)
 * @paper 7-11 阶段 2 的答案其实只需要算规则里的，我们过滤一下(ok)
 * @paper 7-11 优化阶段 1 的读文件逻辑，现在判断太多了，我们可以抽离(ok)
 * @paper 7-11 求解 P99 其实可以转换成求 P01，即从大到小排序，然后拿 1% 位置的值(ok)
 * @paper 7-13 拓扑 + DP 即可一次性找到所有节点的最长路径，线下找到最短路仅需 0ms(ok)
 * @paper 7-13 提前维护一阶段的答案最大最小值，优化第一阶段答案的计算(ok)
 * @paper 7-14 查询自适应，动态的根据哈希是否冲突选择何种的查询方式。(ok)
 * @paper 7-15 双线程流水线处理(生产者-消费者模式)(ok)
 * @paper 7-15 将数据解析提取出来，添加一条流水线，构成读取->解析->计算流水线(ok)
 * @paper 7-16 使用查询的所有属性计算哈希值作为 key，动态的找出一个适合的令其不冲突的魔数(ok)
 * @paper 7-16 加入第二阶段查询预热(ok)
 *
 */
@SuppressWarnings({"StatementWithEmptyBody", "unchecked"})
public class KcodeAlertAnalysisImpl implements KcodeAlertAnalysis {

    /* ---------------- 界限值 ---------------- */
    private final int DATA_BUFFER_LIMIT = 3;            /* 数据缓冲池的最大数量 */
    private final int READ_BLOCK_LIMIT = 2;             /* 读取块最大数量，要注意内存的使用 */

    /* ---------------- 类型 ---------------- */
    private final int TYPE_P99 = 3;
    private final int TYPE_SR = 2;

    /* ---------------- 快速查询 ---------------- */
    private int MAGIC_FAST_QUERY;       /* 动态魔数，用于阶段 2 查询 */

    /* ---------------- 多线程 ---------------- */
    private BlockingQueue<ParsingAccessory> readingQueue = new LinkedBlockingDeque<>(READ_BLOCK_LIMIT);
    private BlockingQueue<ParsingAccessory> parsingQueue = new LinkedBlockingDeque<>(READ_BLOCK_LIMIT);
    private BlockingQueue<ComputingAccessory> computingQueue = new LinkedBlockingDeque<>(100);
    private CountDownLatch computingLatch = new CountDownLatch(1);
    private int currComputingTimeIndex = 1;

    /* ---------------- 答案 ---------------- */
    private Map<ServiceIpPair, ServiceIpPairAnswer[]> serviceIpPairAnswerMap = new HashMap<>(
            500, 0.5f
    );
    private Map<Integer, PairData>[] serviceIpPairDataMap
            = new Map[2];  /* 服务 IP 对数据，时间有交叉分支，所以需要数组 */
    private List<String> alarmAnswer = new ArrayList<>(300);
    private Map<Integer, Map<Integer, PairData[]>> servicePairData = new HashMap<>(
            300, 0.5f
    );  /* 服务对数据 */
    private Map<Q2Key, List<String>> longestPathAnswer = new HashMap<>(500, 0.5f);
    private List<String>[] magicLongestPathAnswer;     /* 阶段 2 答案，将在之后初始化 */


    /* ---------------- 映射 ---------------- */
    private Map<String, Integer> serviceNodeMap = new HashMap<>(
            300, 0.5f
    );  /* 服务节点（索引）映射表 */
    private String[] serviceTable;      /* 之后获取到相关值再进行初始化 */
    private int numberOfNode;           /* 节点数量 */
    private int originTimeEigenvalue;   /* 第一条记录的时间特征值 */
    private int maxTimeIndex;           /* 最大时间索引 */

    /* ---------------- 数据 ---------------- */
    private Set<ServiceIpPair> serviceIpPairSet = new HashSet<>(500);
    private ServiceIpPair[] serviceIpPairs;
    private Set<ServicePair> servicePairSet = new HashSet<>(300);
    private String[] timeIndex2StringTable;                     /* 时间索引 -> 字符串 */
    private Graph outGraph = new Graph();                       /* 出图，有向无权图 */
    private Graph inGraph = new Graph();                        /* 入图 */
    private int nextNode;
    private Set<Integer> dimRulesOfCaller = new HashSet<>(100);             /* 模糊报警规则 */
    private Set<Integer> dimRulesOfResponder = new HashSet<>(100);             /* 模糊报警规则 */
    private Set<Integer> servicePairRules = new HashSet<>(100);     /* 准确报警规则 */
    private Map<Integer, PairData>[] serviceIpPairDataBuffer
            = new Map[DATA_BUFFER_LIMIT];   /* 服务 IP 对数据缓冲池，为了复用数据 */

    public KcodeAlertAnalysisImpl() {
        /* 初始化服务 IP 对数据缓冲池 */
        for(int i = 0; i < DATA_BUFFER_LIMIT; ++i) {
            serviceIpPairDataBuffer[i] = new HashMap<>(500, 0.5f);
        }

        /* 初始化分钟交叉数据区域 */
        serviceIpPairDataMap[0] = serviceIpPairDataBuffer[0];
        serviceIpPairDataMap[1] = serviceIpPairDataBuffer[1];

        /* 解析线程(消费者) */
        new Thread(() -> {
            /* 读取属性 */
            int callerIP;
            int responderIP;
            int success;
            int elapsedTime;
            int dataZone;
            int startTimeEigenvalue, currTimeEigenvalue;
            int nextPairData = 0;               /* 指向服务 IP 对数据缓冲池中的下一个空闲项 */
            /* 数据块属性 */
            byte[] handleBuffer;
            int handleIndex;
            int handleLength;
            int num;
            byte currByte;
            ParsingAccessory parsingAccessory;
            try{
                /* 先初始化 currTimeEigenvalue，我们规定第一个传过来的解析配件必须携带此信息 */
                currTimeEigenvalue = parsingQueue.take().getCurrTimeEigenvalue();

                /* 等待解析配件，并解析数据 */
                while ( !(parsingAccessory = parsingQueue.take()).isStopSignal() ) {
                    /* 初始化信息 */
                    handleBuffer = parsingAccessory.getHandleBuffer();
                    handleLength = parsingAccessory.getHandleLength();

                    /* 开始解析字节数据 */
                    handleIndex = 0;
                    while (handleIndex < handleLength) {
                        /* 调用者 */
                        handleIndex += 3;
                        while ( (handleBuffer[handleIndex++]) != ',' ) {  }

                        /* 调用者 IP */
                        handleIndex += 4;       /* IP 前缀一定是 "10."  */
                        callerIP = num = 0;
                        while ( (handleBuffer[handleIndex++]) != '.' ) {  }
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
                        handleIndex += 3;
                        while ( (handleBuffer[handleIndex++]) != ',' ) {  }

                        /* 响应者 IP */
                        handleIndex += 4;       /* IP 前缀一定是 "10."  */
                        responderIP = num = 0;
                        while ( (handleBuffer[handleIndex++]) != '.' ) {  }
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
                        success = (handleBuffer[handleIndex] == 't' ? 1 : 0);
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
                        handleIndex += 5;      /* 跳过后面的剩余 5 位 */

                        /* 到了下一分钟？ */
                        if(startTimeEigenvalue - currTimeEigenvalue == 6) {
                            /* 计算 */
                            dataZone = currComputingTimeIndex % 2;
                            computingQueue.offer(new ComputingAccessory(serviceIpPairDataMap[dataZone], currComputingTimeIndex));
                            if(DATA_BUFFER_LIMIT == nextPairData) nextPairData = 0; /* 重新开始复用 */
                            serviceIpPairDataMap[dataZone] = serviceIpPairDataBuffer[nextPairData++];  /* 别忘了从数据缓冲池里拿新的 */
                            /* 更新当前时间 */
                            currComputingTimeIndex++;
                            currTimeEigenvalue = startTimeEigenvalue;
                        }

                        /* 加入到数据区域 */
                        serviceIpPairDataMap[((startTimeEigenvalue - originTimeEigenvalue) / 6) % 2]
                                .get((callerIP << 16) | responderIP).add(elapsedTime, success);

                    }

                    /* 解析完成，将这个数据配件返回到读取队列中 */
                    readingQueue.offer(parsingAccessory);
                }
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }).start();

        /* 计算线程(消费者) */
        new Thread(() -> {
            ComputingAccessory computingAccessory;
            int timeIndex;
            try{
                while ( (computingAccessory = computingQueue.take()).getTimeIndex() != -1 ) {
                    Map<Integer, PairData> onZone = computingAccessory.getDataMap();
                    timeIndex = computingAccessory.getTimeIndex();
                    /* 遍历所有主被调 IP 对，它们将指引我们应该从哪拿到数据并计算 */
                    for(ServiceIpPair sp : serviceIpPairs) {
                        PairData data = onZone.get(sp.getIpPair());
                        if(data != null && data.size() > 0) {
                            /* 汇总服务对的数据，阶段 2 需要 */
                            PairData[] sOnTime = servicePairData.get(serviceNodeMap.get(sp.getCaller()))
                                    .get(serviceNodeMap.get(sp.getResponder()));
                            PairData sdata = sOnTime[timeIndex];
                            if(sdata == null) sdata = sOnTime[timeIndex] = new PairData();
                            sdata.addAll(data);

                            /* 添加答案 */
                            ServiceIpPairAnswer[] onTime = serviceIpPairAnswerMap.get(sp);
                            ServiceIpPairAnswer answer = onTime[timeIndex];
                            if(answer == null) {
                                answer = onTime[timeIndex] = new ServiceIpPairAnswer();
                            }
                            answer.add(new ServiceIpPairAnswer.Answer(data.getP99(), data.getSuccessRate()
                                    , sp.getCallerIp(), sp.getResponderIP()));
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                computingLatch.countDown();
            }
        }).start();
    }

    /**
     * 线下数据集信息：
     *  nr_ip_pair
     *  4165/?
     */
    @Override
    public Collection<String> alarmMonitor(String path, Collection<String> alertRules) throws Exception {
        /* 获取文件信息 */
        FileChannel channel = new RandomAccessFile(path, "r").getChannel();

        /* 初始化当前时间 */
        BufferedReader inputStream = new BufferedReader(new FileReader(path));
        long timeMs = Long.parseLong(inputStream.readLine().split(",")[6]);
        /* 第一条记录的时间 */
        int originTime = (int) (timeMs / (1000 * 60));
        inputStream.close();
        /* 快速算出 xx:00 分的特征值 */
        String originTimeMsString = String.valueOf(originTime * 60L * 1000L);
        originTimeEigenvalue = originTimeEigenvalue * 10 + originTimeMsString.charAt(4) - '0';
        originTimeEigenvalue = originTimeEigenvalue * 10 + originTimeMsString.charAt(5) - '0';
        originTimeEigenvalue = originTimeEigenvalue * 10 + originTimeMsString.charAt(6) - '0';
        originTimeEigenvalue = originTimeEigenvalue * 10 + originTimeMsString.charAt(7) - '0';
        originTimeEigenvalue = originTimeEigenvalue * 10 + originTimeMsString.charAt(8) - '0';
        int currTimeEigenvalue = originTimeEigenvalue;

        /* 读取属性 */
        String caller;
        int callerIP;
        String responder;
        int responderIP;
        int success;
        int elapsedTime;
        int callerNode, responderNode;
        ServicePair servicePair;
        /* 块读变量 */
        ParsingAccessory parsingAccessory = new ParsingAccessory();
        byte[] remaining = new byte[256 << 10];
        int remainingLength = 0;    /* 上次剩余 */
        byte[] handleBuffer = parsingAccessory.getHandleBuffer();
        int handleLength = 0, prevHandleIndex = 0;
        ByteBuffer readBuffer = ByteBuffer.allocate(256 << 10);
        byte[] readBytes;
        int readCount, handleTotal = 0;
        boolean findOut = false;
        /* 其他变量 */
        byte currByte;
        int num, handleIndex, pos;
        byte[] stringBuffer = new byte[64];
        int stringLength;
        int startTimeEigenvalue;/* 开始时间特征值 */
        /* 先找前两分钟，它可以唯一确定很多信息 */
        while ( !findOut ) {
            readCount = channel.read(readBuffer);       /* 一次从文件通道中读一块 */
            /* 拷贝上次剩余字节到本次处理缓冲区 */
            readBytes = readBuffer.array();
            System.arraycopy(remaining, 0, handleBuffer, 0, remainingLength);

            /* 从缓冲区尾部找到一条记录，换行会告诉我们在哪 */
            pos = readCount - 1;
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

            /* 开始处理准备好的数据 */
            handleIndex = 0;
            while (handleIndex < handleLength) {
                /* 调用者 */
                stringLength = 0;
                while ( (currByte = handleBuffer[handleIndex++]) != ',' ) {
                    stringBuffer[stringLength++] = currByte;
                }
                caller = new String(stringBuffer, 0, stringLength);

                /* 调用者 IP */
                handleIndex += 3;   /* IP 前缀一定是 "10."  */
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
                stringLength = 0;
                while ( (currByte = handleBuffer[handleIndex++]) != ',' ) {
                    stringBuffer[stringLength++] = currByte;
                }
                responder = new String(stringBuffer, 0, stringLength);

                /* 响应者 IP */
                handleIndex += 3;   /* IP 前缀一定是 "10."  */
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
                success = (handleBuffer[handleIndex] == 't' ? 1 : 0);
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
                handleIndex += 5;      /* 跳过后面的剩余 5 位 */

                /* 乱序正负一分钟，所以这里是判断是否到了下下分钟 */
                if(startTimeEigenvalue - currTimeEigenvalue == 12) {
                    /* 直接结束，已经找到所有所需信息 */
                    currTimeEigenvalue = startTimeEigenvalue;
                    findOut = true;
                    break;
                }
                prevHandleIndex = handleIndex;

                ServiceIpPair pair = new ServiceIpPair(caller, responder, callerIP, responderIP);
                /* 添加服务索引 */
                serviceNodeMap.computeIfAbsent(caller, k -> nextNode++);
                serviceNodeMap.computeIfAbsent(responder, k -> nextNode++);
                /* 初始化一些数据和答案映射 */
                callerNode = serviceNodeMap.get(caller);
                responderNode = serviceNodeMap.get(responder);
                serviceIpPairAnswerMap.computeIfAbsent(pair, k -> new ServiceIpPairAnswer[100]);
                servicePairData.computeIfAbsent(callerNode, k -> new HashMap<>())
                        .computeIfAbsent(responderNode, k -> new PairData[100]);
                /* 建图，使用已经映射好的索引作为节点，一个出图，一个入图 */
                servicePair = new ServicePair(caller, responder);
                if(!servicePairSet.contains(servicePair)) {
                    outGraph.addEdge(callerNode, responderNode);
                    inGraph.addEdge(responderNode, callerNode);
                    servicePairSet.add(servicePair);
                }
                /* 将主被调 IP 对加入到集合 */
                serviceIpPairSet.add(pair);
                /* 加入到数据区域 */
                serviceIpPairDataMap[((startTimeEigenvalue - originTimeEigenvalue) / 6) % 2]
                        .computeIfAbsent(pair.getIpPair(), k -> new PairData()).add(elapsedTime, success);
            }
        }

        /* 准备服务 IP 对数组 */
        num = serviceIpPairSet.size();
        serviceIpPairs = serviceIpPairSet.toArray(new ServiceIpPair[num]);
        /* 初始化一阶段数据区域 */
        for(ServiceIpPair sp : serviceIpPairs) {
            for(num = 0; num < DATA_BUFFER_LIMIT; ++num) {
                serviceIpPairDataBuffer[num].computeIfAbsent(sp.getIpPair(), k -> new PairData());
            }
        }

        /* 提交计算任务：第一分钟 */
        computingQueue.offer(new ComputingAccessory(serviceIpPairDataMap[0], 0));
        serviceIpPairDataMap[0] = serviceIpPairDataBuffer[2];  /* 从缓冲池里拿新的 */
        numberOfNode = serviceNodeMap.size();

        /* 初始化服务映射表， index -> 字符串 */
        serviceTable = new String[numberOfNode];
        for(Map.Entry<String, Integer> e : serviceNodeMap.entrySet()) {
            serviceTable[e.getValue()] = e.getKey();
        }

        /* 接着读取剩下的 */
        readingQueue.offer(parsingAccessory);
        for(num = 0; num < READ_BLOCK_LIMIT - 1; ++num) {   /* 读取工厂初始可使用配件，这些配件和解析线程共用 */
            readingQueue.offer(new ParsingAccessory());
        }
        parsingAccessory.setCurrTimeEigenvalue(currTimeEigenvalue);
        parsingQueue.offer(parsingAccessory);       /* 提交这一个配件用于初始化当前时间信息 */
        channel.position(handleTotal - (handleLength - prevHandleIndex));   /* 处理到了这里 */
        remainingLength = 0;
        while ( true ) {
            parsingAccessory = readingQueue.take();     /* 从队列中获取可用的配件 */
            if( (readCount = channel.read(readBuffer)) <= 0 ) break;   /* 一次从文件通道中读一块 */

            /* 拷贝上次剩余字节到本次处理缓冲区 */
            readBytes = readBuffer.array();
            handleBuffer = parsingAccessory.getHandleBuffer();
            System.arraycopy(remaining, 0, handleBuffer, 0, remainingLength);

            /* 从缓冲区尾部找到一条记录，换行会告诉我们在哪 */
            pos = readCount - 1;
            while (readBytes[pos--] != '\n') { }
            pos += 2;      /* 保证处于换行后 */
            parsingAccessory.setHandleLength(pos + remainingLength);

            /* 拷贝本次数据到处理缓冲中 */
            System.arraycopy(readBytes, 0, handleBuffer, remainingLength, pos);

            /* 拷贝剩余的数据(不能完整处理)到剩余缓冲区中 */
            remainingLength = readCount - pos;
            System.arraycopy(readBytes, pos, remaining, 0, remainingLength);
            readBuffer.position(0);

            /* 提交本次读取的数据块给解析工厂进行解析 */
            parsingQueue.offer(parsingAccessory);
        }

        /* 通知解析工厂停止，已经没有活干了 */
        parsingAccessory.setStopSignal(true);
        parsingQueue.offer(parsingAccessory);

        /* 计算剩余的两分钟，第一分钟给计算线程，最后一分钟由主线程处理，很奈斯～ */
        computingQueue.offer(new ComputingAccessory(serviceIpPairDataMap[currComputingTimeIndex % 2], currComputingTimeIndex));
        computingQueue.offer(new ComputingAccessory(null, -1));   /* 通知计算线程结束 */
        pairAnswerCompute(++currComputingTimeIndex, currComputingTimeIndex % 2);
        maxTimeIndex = currComputingTimeIndex;       /* 最大的时间索引 */

        /* 初始化时间字符串数组 */
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        timeIndex2StringTable = new String[maxTimeIndex + 1];
        for(num = 0; num <= maxTimeIndex; ++num) {
            timeIndex2StringTable[num] = dateFormat.format((originTime + num) * 60L * 1000L);
        }

        /* 计算所有报警值，一阶段 */
        computingLatch.await();     /* 注意： 计算工厂停止工作才能继续 */
        alarmAnswerCompute(alertRules);

        /* 计算所有最长路径答案，二阶段 */
        longestPathAnswerCompute();

        return alarmAnswer;
    }

    /**
     * 计算所有的报警规则的报警值
     *
     * @param alertRules 所有报警规则，示例如下
     *  1,ALL,rd_9563486881659901967,P99,2>,100ms
     *  9,rd_14847304958688727175,rd_467962675963435152,P99,1>,100ms
     */
    private void alarmAnswerCompute(Collection<String> alertRules) {
        int nr;             /* 规则编号 */
        String caller;      /* 调用者 */
        int callerNode;
        String responder;   /* 响应者 */
        int responderNode;
        int alertType;      /* 报警类型，P99 或 成功率 */
        int duration;       /* 持续时间 */
        boolean greater;    /* 大于？ */
        int alertValue;     /* 报警值 */
        /* 遍历所有报警规则 */
        for(String rule : alertRules) {
            /* 先解析报警规则属性 */
            String[] split = rule.split(",");
            nr = Integer.parseInt(split[0]);
            caller = (split[1].charAt(0) != 'A' ? split[1] : null);     /* 为 null 表示 ALL */
            callerNode = (caller != null ? serviceNodeMap.get(caller) : -1);
            responder = (split[2].charAt(0) != 'A' ? split[2] : null);  /* 为 null 表示 ALL */
            responderNode = (responder != null ? serviceNodeMap.get(responder) : -1);
            alertType = (split[3].charAt(0) == 'P' ? TYPE_P99 : TYPE_SR);
            String durationString = split[4].substring(0, split[4].length() - 1);
            duration = Integer.parseInt(durationString);
            greater = (split[4].charAt(split[4].length() - 1) == '>');
            if(alertType == TYPE_P99) {
                alertValue = Integer.parseInt(split[5].replace("ms", ""));
            } else {
                String SRS = split[5].replace("%", "");
                String[] STSS = SRS.split("\\.");   /* 以小数点分割 */
                if(STSS.length == 1) {  /* 没有小数位 */
                    alertValue = Integer.parseInt(SRS) * 100;
                } else {
                    int decimal = Integer.parseInt(STSS[1]);
                    if(STSS[1].length() == 1) decimal *= 10;    /* 99.9，我们为 .9 补齐 0 */
                    alertValue = Integer.parseInt(STSS[0]) * 100 + decimal;
                }
            }

            /* 现在，根据规则去所有的主被调对数据中寻找存在报警的主被调对 */
            if(caller != null && responder != null) {   /* 准确的规则 */
                /* 添加一个准确规则*/
                servicePairRules.add(callerNode * numberOfNode + responderNode);

                /* 该层遍历所有调用者 */
                for(Map.Entry<ServiceIpPair, ServiceIpPairAnswer[]> entry : serviceIpPairAnswerMap.entrySet()) {
                    ServiceIpPair mp = entry.getKey();
                    if(!mp.getCaller().equals(caller) || !mp.getResponder().equals(responder)) continue;  /* 响应者不匹配 */
                    validationRule(nr, alertType, duration, greater, alertValue, caller, responder, entry.getValue());
                }

            } else {                                    /* 含有 ALL 的模糊规则 */

                if(caller == null) {                    /* 调用者模糊 */
                    /* 添加一个模糊规则*/
                    dimRulesOfResponder.add(responderNode);

                    /* 遍历所有主被调 IP 对的 P99 以及 SR */
                    for(Map.Entry<ServiceIpPair, ServiceIpPairAnswer[]> entry : serviceIpPairAnswerMap.entrySet()) {
                        ServiceIpPair sp = entry.getKey();
                        if(!sp.getResponder().equals(responder)) continue;  /* 响应者不匹配 */
                        validationRule(nr, alertType, duration, greater, alertValue, sp.getCaller(), responder, entry.getValue());
                    }

                } else {               /* 响应者模糊 */
                    /* 添加一个模糊规则 */
                    dimRulesOfCaller.add(callerNode);

                    /* 遍历所有主被调 IP 对的 P99 以及 SR */
                    for(Map.Entry<ServiceIpPair, ServiceIpPairAnswer[]> entry : serviceIpPairAnswerMap.entrySet()) {
                        ServiceIpPair sp = entry.getKey();
                        if(!sp.getCaller().equals(caller)) continue;  /* 调用者不匹配 */
                        validationRule(nr, alertType, duration, greater, alertValue, caller, sp.getResponder(), entry.getValue());
                    }

                }
            }
        }
    }

    /**
     * 验证报警规则
     */
    private void validationRule(int nr, int alertType, int duration, boolean greater
            , int alertValue, String caller, String responder, ServiceIpPairAnswer[] onTime) {
        int persist = 0;    /* 报警持续时间 */
        int timeIndex, callerIp, responderIp, P99, SR, decimal;
        String ipPrefix = "10.", ms = "ms";
        if(greater) { /* > */

            if(alertType == TYPE_P99) { /* P99 */
                for (timeIndex = 0; timeIndex <= maxTimeIndex; ++timeIndex) {
                    ServiceIpPairAnswer answers = onTime[timeIndex];
                    if (answers == null || answers.size() == 0) {
                        persist = 0;
                        continue;   /* 该主被调 IP 对在该分钟没有答案 */
                    }

                    /* 通过该分钟的最大值来判断该分钟是否报警 */
                    if(answers.getMaxP99() > alertValue) {
                        if(++persist >= duration) { /* 该分钟已报警 */
                            for(ServiceIpPairAnswer.Answer ans : answers.getAnswers()) {
                                P99 = ans.getP99();
                                if(P99 > alertValue) {
                                    StringBuilder answerBuilder = new StringBuilder();
                                    callerIp = ans.getCallerIp();
                                    responderIp = ans.getResponderIp();
                                    answerBuilder.append(nr).append(',').append(timeIndex2StringTable[timeIndex]).append(',').append(caller)
                                            .append(',').append(ipPrefix).append(((callerIp & 0xFFFFFF) >> 16))
                                            .append('.').append(((callerIp & 0xFFFF) >> 8)).append('.').append((callerIp & 0xff))
                                            .append(',').append(responder).append(',').append(ipPrefix)
                                            .append(((responderIp & 0xFFFFFF) >> 16)).append('.').append(((responderIp & 0xFFFF) >> 8))
                                            .append('.').append((responderIp & 0xff)).append(',').append(P99).append(ms);
                                    alarmAnswer.add(answerBuilder.toString());
                                }
                            }
                        }
                    } else {
                        persist = 0;        /* 报警持续已中断 */
                    }
                }

            } else {    /* SR */

                for (timeIndex = 0; timeIndex <= maxTimeIndex; ++timeIndex) {
                    ServiceIpPairAnswer answers = onTime[timeIndex];
                    if (answers == null || answers.size() == 0) {
                        persist = 0;
                        continue;   /* 该主被调 IP 对在该分钟没有答案 */
                    }

                    /* 通过该分钟的最大值来判断该分钟是否报警 */
                    if(answers.getMaxSR() > alertValue) {
                        if(++persist >= duration) { /* 该分钟已报警 */
                            for(ServiceIpPairAnswer.Answer ans : answers.getAnswers()) {
                                SR = ans.getSR();
                                if(SR > alertValue) {
                                    /* 添加答案字符串 */
                                    StringBuilder answerBuilder = new StringBuilder();
                                    callerIp = ans.getCallerIp();
                                    responderIp = ans.getResponderIp();
                                    decimal = SR % 100;         /* 小数位，只有两位 */
                                    answerBuilder.append(nr).append(',').append(timeIndex2StringTable[timeIndex]).append(',').append(caller)
                                            .append(',').append(ipPrefix).append(((callerIp & 0xFFFFFF) >> 16))
                                            .append('.').append(((callerIp & 0xFFFF) >> 8)).append('.').append((callerIp & 0xff))
                                            .append(',').append(responder).append(',').append(ipPrefix)
                                            .append(((responderIp & 0xFFFFFF) >> 16)).append('.').append(((responderIp & 0xFFFF) >> 8))
                                            .append('.').append((responderIp & 0xff)).append(',').append(SR / 100).append('.')
                                            .append(decimal / 10).append(decimal % 10).append('%');
                                    alarmAnswer.add(answerBuilder.toString());
                                }
                            }
                        }
                    } else {
                        persist = 0;        /* 报警持续已中断 */
                    }
                }
            }

        } else {    /* < */

            if(alertType == TYPE_P99) { /* P99 */
                for (timeIndex = 0; timeIndex <= maxTimeIndex; ++timeIndex) {
                    ServiceIpPairAnswer answers = onTime[timeIndex];
                    if (answers == null || answers.size() == 0) {
                        persist = 0;
                        continue;   /* 该主被调 IP 对在该分钟没有答案 */
                    }

                    /* 通过该分钟的最小值来判断该分钟是否报警 */
                    if(answers.getMinP99() < alertValue) {
                        if(++persist >= duration) { /* 该分钟已报警 */
                            for(ServiceIpPairAnswer.Answer ans : answers.getAnswers()) {
                                P99 = ans.getP99();
                                if(P99 < alertValue) {
                                    StringBuilder answerBuilder = new StringBuilder();
                                    callerIp = ans.getCallerIp();
                                    responderIp = ans.getResponderIp();
                                    answerBuilder.append(nr).append(',').append(timeIndex2StringTable[timeIndex]).append(',').append(caller)
                                            .append(',').append(ipPrefix).append(((callerIp & 0xFFFFFF) >> 16))
                                            .append('.').append(((callerIp & 0xFFFF) >> 8)).append('.').append((callerIp & 0xff))
                                            .append(',').append(responder).append(',').append(ipPrefix)
                                            .append(((responderIp & 0xFFFFFF) >> 16)).append('.').append(((responderIp & 0xFFFF) >> 8))
                                            .append('.').append((responderIp & 0xff)).append(',').append(P99).append(ms);
                                    alarmAnswer.add(answerBuilder.toString());
                                }
                            }
                        }
                    } else {
                        persist = 0;        /* 报警持续已中断 */
                    }
                }
            } else {    /* SR */

                for (timeIndex = 0; timeIndex <= maxTimeIndex; ++timeIndex) {
                    ServiceIpPairAnswer answers = onTime[timeIndex];
                    if (answers == null || answers.size() == 0) {
                        persist = 0;
                        continue;   /* 该主被调 IP 对在该分钟没有答案 */
                    }

                    /* 通过该分钟的最小值来判断该分钟是否报警 */
                    if(answers.getMinSR() < alertValue) {
                        if(++persist >= duration) { /* 该分钟已报警 */
                            for (ServiceIpPairAnswer.Answer ans : answers.getAnswers()) {
                                SR = ans.getSR();
                                if(SR >= 0 && SR < alertValue) {
                                    /* 添加答案字符串 */
                                    StringBuilder answerBuilder = new StringBuilder();
                                    callerIp = ans.getCallerIp();
                                    responderIp = ans.getResponderIp();
                                    decimal = SR % 100;         /* 小数位，只有两位 */
                                    answerBuilder.append(nr).append(',').append(timeIndex2StringTable[timeIndex]).append(',').append(caller)
                                            .append(',').append(ipPrefix).append(((callerIp & 0xFFFFFF) >> 16))
                                            .append('.').append(((callerIp & 0xFFFF) >> 8)).append('.').append((callerIp & 0xff))
                                            .append(',').append(responder).append(',').append(ipPrefix)
                                            .append(((responderIp & 0xFFFFFF) >> 16)).append('.').append(((responderIp & 0xFFFF) >> 8))
                                            .append('.').append((responderIp & 0xff)).append(',').append(SR / 100).append('.')
                                            .append(decimal / 10).append(decimal % 10).append('%');
                                    alarmAnswer.add(answerBuilder.toString());
                                }
                            }
                        }
                    } else {
                        persist = 0;        /* 报警持续已中断 */
                    }
                }
            }
        }
    }

    /**
     * 计算一个主被调对在某一时刻(分)的答案
     * @param timeIndex 开始时间索引(分)
     */
    private void pairAnswerCompute(int timeIndex, int dataZone) {
        Map<Integer, PairData> onZone = serviceIpPairDataMap[dataZone];
        /* 遍历所有主被调 IP 对，它们将指引我们应该从哪拿到数据并计算 */
        for(ServiceIpPair sp : serviceIpPairs) {
            PairData data = onZone.get(sp.getIpPair());
            if(data != null && data.size() > 0) {
                /* 汇总服务对的数据，阶段 2 需要 */
                PairData[] sOnTime = servicePairData.get(serviceNodeMap.get(sp.getCaller()))
                        .get(serviceNodeMap.get(sp.getResponder()));
                PairData sdata = sOnTime[timeIndex];
                if(sdata == null) sdata = sOnTime[timeIndex] = new PairData();
                sdata.addAll(data);

                /* 添加答案 */
                ServiceIpPairAnswer[] onTime = serviceIpPairAnswerMap.get(sp);
                ServiceIpPairAnswer answer = onTime[timeIndex];
                if(answer == null) {
                    answer = onTime[timeIndex] = new ServiceIpPairAnswer();
                }
                answer.add(new ServiceIpPairAnswer.Answer(data.getP99(), data.getSuccessRate()
                        , sp.getCallerIp(), sp.getResponderIP()));
            }
        }
    }

    /**
     * 计算所有最长路径答案
     */
    private void longestPathAnswerCompute() {
        /* 初始化一些数据和变量 */
        String caller, responder;
        int endNode, lastNode = 0;
        int callerNode, responderNode, timeIndex, servicePair;
        int i, j, k, l, v, SR, decimal;
        String P99S = "P99";
        String SRS = "SR";
        String errorP99 = "-1ms", errorSR = "-1%";
        String errorP99cc = "-1ms,", errorSRcc = "-1%,";
        String nexts = "->", mss = "ms";
        String msscc = "ms,", percentscc = "%,";
        Queue<Integer> Q = new LinkedList<>();
        int[] outTable = new int[numberOfNode];
        List<FlyanIntArrayList>[] nodesLongestPathsOfLeft = new List[numberOfNode];
        List<FlyanIntArrayList>[] nodesLongestPathsOfRight = new List[numberOfNode];
        for(i = 0; i < numberOfNode; ++i) {
            nodesLongestPathsOfLeft[i] = new ArrayList<>();
            nodesLongestPathsOfRight[i] = new ArrayList<>();
        }

        /* 对所有出度为 0 的点进行一次拓扑 + DP，即可获取到所有节点的最长链，
         * 要拿到调用者左边和右边的的最长链，所以是拓扑两次。
         *
         * PS: 这个过程足够快，线下表现没有一个数据集是超过 1ms 的，主要的耗时是后面
         * 为每个主被调对拼接答案，因为它需要对以前汇总的数据进行计算 P99。
         */
        long startNs = System.nanoTime();
        topological(inGraph, outGraph, nodesLongestPathsOfLeft, Q, outTable);  /* 注意反向图 */
        topological(outGraph, inGraph, nodesLongestPathsOfRight, Q, outTable);  /* 注意反向图 */
        System.out.println((System.nanoTime() - startNs) + "ns");

        /* 遍历所有服务对 */
        for(ServicePair sp : servicePairSet) {
            caller = sp.getCaller();
            responder = sp.getResponder();
            callerNode = serviceNodeMap.get(caller);
            responderNode = serviceNodeMap.get(responder);
            servicePair = callerNode * numberOfNode + responderNode;

            /* 检查本服务调对否符合规则，不符合跳过 */
            if(!(dimRulesOfCaller.contains(callerNode) || dimRulesOfResponder.contains(responderNode)
                    || servicePairRules.contains(servicePair))) continue;

            /* 这层遍历为了所有时间段 */
            for (timeIndex = 0; timeIndex <= maxTimeIndex; ++timeIndex) {
                List<String> answerByP99 = new ArrayList<>(100);
                List<String> answerBySR = new ArrayList<>(100);

                /* 拼接左边右边，得到整体的最长链。 */
                List<FlyanIntArrayList> longestPathOfLeft = nodesLongestPathsOfLeft[callerNode];
                List<FlyanIntArrayList> longestPathOfRight = nodesLongestPathsOfRight[responderNode];
                for(i = 0; i < longestPathOfLeft.size(); ++i) {
                    for (j = 0; j < longestPathOfRight.size(); ++j) {
                        StringBuilder suffixByP99 = new StringBuilder();
                        StringBuilder suffixBySR = new StringBuilder();
                        StringBuilder answerBuilderByP99 = new StringBuilder();
                        StringBuilder answerBuilderBySR;
                        FlyanIntArrayList leftLongestPath = longestPathOfLeft.get(i);
                        FlyanIntArrayList rightLongestPath = longestPathOfRight.get(j);
                        FlyanIntArrayList longest = new FlyanIntArrayList(100);

                        /* 拼接最长路，注意左边需要反向 */
                        for(k = leftLongestPath.size() - 1; k >= 0; --k) {
                            v = leftLongestPath.get(k);
                            longest.add(v);
                        }
                        longest.addAll(rightLongestPath.getData(), rightLongestPath.size());   /* 右边 */

                        l = 0;
                        for(k = 0; k < longest.size() - 1; ++k) {
                            v = longest.get(k);
                            answerBuilderByP99.append(serviceTable[v]).append(nexts);
                            if(l++ > 0) {  /* 不是第一次，那么生成相应的后缀答案 */
                                PairData data = getServicePairData(lastNode, v, timeIndex);
                                if(data != null && data.size() > 0) {  /* 该分钟存在调用 */
                                    /* 首先存放 P99 的 */
                                    suffixByP99.append(data.getSafeP99()).append(msscc);
                                    /* 然后是 SR */
                                    SR = data.getSafeSuccessRate();
                                    decimal = SR % 100;         /* 小数位，只有两位 */
                                    suffixBySR.append(SR / 100).append('.').append(decimal / 10).append(decimal % 10).append(percentscc);
                                } else {            /* 该分钟无调用 */
                                    suffixByP99.append(errorP99cc);
                                    suffixBySR.append(errorSRcc);
                                }
                            }
                            lastNode = v;
                        }
                        endNode = longest.get(longest.size() - 1);
                        answerBuilderByP99.append(serviceTable[endNode]);
                        PairData data = getServicePairData(lastNode, endNode, timeIndex);
                        if(data != null && data.size() > 0) {  /* 该分钟存在调用 */
                            /* 首先存放 P99 的 */
                            suffixByP99.append(data.getSafeP99()).append(mss);
                            /* 然后是 SR */
                            SR = data.getSafeSuccessRate();
                            decimal = SR % 100;         /* 小数位，只有两位 */
                            suffixBySR.append(SR / 100).append('.').append(decimal / 10).append(decimal % 10).append('%');
                        } else {            /* 该分钟无调用 */
                            suffixByP99.append(errorP99);
                            suffixBySR.append(errorSR);
                        }
                        answerBuilderBySR = new StringBuilder(answerBuilderByP99);
                        answerBuilderByP99.append('|').append(suffixByP99);
                        answerBuilderBySR.append('|').append(suffixBySR);
                        answerByP99.add(answerBuilderByP99.toString());
                        answerBySR.add(answerBuilderBySR.toString());
                    }
                }
                /* 添加该准备调对在该分钟的答案 */
                longestPathAnswer.put(new Q2Key(caller, responder, timeIndex2StringTable[timeIndex], P99S), answerByP99);
                longestPathAnswer.put(new Q2Key(caller, responder, timeIndex2StringTable[timeIndex], SRS), answerBySR);
            }

        }

        /* 答案已全部找到，现在动态寻找阶段 2 的快速查询魔数 */
        findMagicFastQuery();

        /* 查询预热 */
        for(i = 0; i < 10000; ++i) {
            for(Q2Key key : longestPathAnswer.keySet()) {
                List<String> answer = magicLongestPathAnswer[key.dynicMagicIndex(MAGIC_FAST_QUERY)];
            }
        }
    }

    /**
     * 动态寻找阶段 2 的快速查询魔数，我们已经确定了答案，这里只需要花点时间就可以大幅度加速查询阶段
     */
    private void findMagicFastQuery() {
        /* 初始化一些数据和变量 */
        int magicIndex;
        boolean isCollide;
        final int answerSize = longestPathAnswer.size();
        MAGIC_FAST_QUERY = answerSize;

        /* 一直遍历整个答案，用魔数去比对，一直找到不会冲突的魔数 */
        while (true){
            Set<Integer> magicTestSet = new HashSet<>(answerSize);
            isCollide = false;      /* 假定这轮不会冲突 */

            for(Q2Key k : longestPathAnswer.keySet()) {
                /* 跟魔数进行比对，如果产生冲突则跳出 */
                magicIndex = k.dynicMagicIndex(MAGIC_FAST_QUERY);
                if(magicTestSet.contains(magicIndex)) { /* 有冲突 */
                    isCollide = true;
                    break;
                }
                magicTestSet.add(magicIndex);
            }

            /* 查看此轮是否产生冲突，没有的话，我们就找到了这个魔数，开始建立魔法答案数组 */
            if(!isCollide) {
                magicLongestPathAnswer = new List[MAGIC_FAST_QUERY + 1];
                for(Map.Entry<Q2Key, List<String>> e : longestPathAnswer.entrySet()) {
                    magicLongestPathAnswer[e.getKey().dynicMagicIndex(MAGIC_FAST_QUERY)] = e.getValue();
                }
                break;
            }
            MAGIC_FAST_QUERY++;
        }
    }

    /**
     * 获取一个主被调对在某时间的数据对象
     */
    private PairData getServicePairData(int callerNode, int responderNode, int timeIndex) {
        Map<Integer, PairData[]> onResponder = servicePairData.get(callerNode);
        if (null == onResponder) return null;   /* 响应者可能是不存在的 */
        PairData[] onTime = onResponder.get(responderNode);
        if(null == onTime) return null;
        return onTime[timeIndex];
    }

    /**
     * 拓扑得到最长路
     *
     * @param graph 在哪张图上进行？
     * @param reverseGraph 反图
     */
    private void topological(Graph graph, Graph reverseGraph, List<FlyanIntArrayList>[] nodesLongestPaths
            , Queue<Integer> Q, int [] outTable) {
        int w, i, v, maxLength;
        System.arraycopy(graph.getOutDegreeTable(), 0, outTable, 0, numberOfNode);
        /* 寻找所有出度为 0 的节点入队列 */
        for(v = 0; v < numberOfNode; ++v) {
            /* 出度 > 0 的我们跳过 */
            if( outTable[v] > 0 ) continue;
            Q.add(v);
        }
        while (!Q.isEmpty()) {
            w = Q.poll();   /* 取出出度为 0 的节点 */
            /* 检查出度
             *  - == 0，最长链就是自己
             *  - > 0，v 最长链 = v + max(v 所有出度的点的最长链)
             */
            if(graph.getOutDegree(w) > 0) {

                /* 遍历 w 的边集，找到拥有最长链的节点 */
                maxLength = 0;
                for(i = graph.getHead(w); i != -1; i = graph.getEdge(i).getNext()) {
                    v = graph.getEdge(i).getV();
                    if(maxLength < nodesLongestPaths[v].get(0).size()) {
                        maxLength = nodesLongestPaths[v].get(0).size();
                    }
                }

                /* 将最长链信息添加 */
                for(i = graph.getHead(w); i != -1; i = graph.getEdge(i).getNext()) {
                    v = graph.getEdge(i).getV();
                    if(maxLength == nodesLongestPaths[v].get(0).size()) {
                        for(FlyanIntArrayList e : nodesLongestPaths[v]) {
                            FlyanIntArrayList lp = new FlyanIntArrayList(50);
                            lp.add(w);      /* 根据公式，包括自己 */
                            lp.addAll(e.getData(), e.size());
                            nodesLongestPaths[w].add(lp);
                        }
                    }
                }

            } else {    /* 出度在这只能是 0 */
                FlyanIntArrayList lp = new FlyanIntArrayList(3);
                lp.add(w);
                nodesLongestPaths[w].add(lp);   /* 只有一条最长路，且这条最长路只包含自己 */
            }

            /* 减少当前节点的前序节点的出度值 */
            for(i = reverseGraph.getHead(w); i != -1; i = reverseGraph.getEdge(i).getNext()) {
                v = reverseGraph.getEdge(i).getV();
                if(--outTable[v] == 0) {    /* 出度被减少为 0，继续加入拓扑队列 */
                    Q.add(v);
                }
            }
        }
    }

    @Override
    public Collection<String> getLongestPath(String caller, String responder, String time, String type) {
        return magicLongestPathAnswer[(29791 * caller.hashCode() + 961 * responder.hashCode()
                + 31 * time.hashCode() + type.hashCode()) & MAGIC_FAST_QUERY];
    }

}
