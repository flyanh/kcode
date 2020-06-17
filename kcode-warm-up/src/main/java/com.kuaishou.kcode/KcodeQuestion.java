package com.kuaishou.kcode;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;

/**
 * @team 辰海飞燕
 * @member 陈元崇 - 济南大学泉城学院
 * @member 张浩宇 - 哈尔滨工业大学
 * @member 王俊杰 - 南京理工大学
 *
 * Created on 2020-06-01
 *
 * 多线程求解
 */
public class KcodeQuestion {

    /* 线程池：拒绝策略为 AbortPolic 策略，直接抛出异常 */
    private final static int NR_THREADS = 4;                      /* 最大线程数量，线下 8C4G */
    //    private final static int NR_THREADS = 16;                     /* 最大线程数量，线上 16C4G */
    private final static int TIME_OUT = 80;                       /* 线程超时时间(s) */
    private final static ExecutorService pool = new ThreadPoolExecutor(NR_THREADS, NR_THREADS, TIME_OUT,
            TimeUnit.SECONDS, new SynchronousQueue<>(), Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.AbortPolicy());
    private static TaskInfo[] infos = new TaskInfo[NR_THREADS];


    /* answer map table */
    private final static int MAX_METHOD_SIZE = 80;          /* 方法跨度 */
    private final static int MAX_TIME_SIZE = 4982;          /* 时间跨度 */
    private final static Answer answer = new Answer();          /* 答案管理对象 */

    /* 数据区域 */
    private final static Set<String> MethodSet = new HashSet<>(MAX_METHOD_SIZE);
    private static String[] methodArray = null;

    /**
     * 答案类，管理答案
     */
    static class Answer {
        private Map<String, Map<Long, String>> answerOfMethodMap =
                new ConcurrentHashMap<>(MAX_METHOD_SIZE);       /* 答案映射 */
        private List<Map<String, FlyArrayList>> methodDataMap
                = new ArrayList<>();                            /* 方法的数据映射 */


        Answer() {
            for(int task = 0; task < NR_THREADS; ++task) {
                methodDataMap.add(new HashMap<>(MAX_METHOD_SIZE));
            }
        }

        /**
         * 添加一个答案到映射中
         *
         * @param methodName 执行方法
         * @param startTime 方法开始执行时间戳，秒级
         * @param answer 答案
         */
        void addAnswer(String methodName, Long startTime, String answer) {
            answerOfMethodMap.computeIfAbsent(methodName, k -> new HashMap<>(MAX_TIME_SIZE))
                    .put(startTime, answer);
        }

        /**
         * 添加一个方法耗时
         *
         * @param methodName 执行方法
         * @param elapsedTime 耗时
         */
        void addElapsedTime(String methodName, int elapsedTime, int task) {
            methodDataMap.get(task).computeIfAbsent(methodName, k -> new FlyArrayList())
                    .add(elapsedTime);
        }

        /**
         * 获取答案
         *
         * @param methodName 执行方法
         * @param startTime 方法开始执行时间戳，秒级
         * @return 答案
         */
        String getAnswer(String methodName, Long startTime) {
            return answerOfMethodMap.get(methodName).get(startTime);
        }

        /**
         * 计算结果
         * @param methodName 执行方法
         * @param startTime 方法开始执行时间戳，秒级
         */
        void compute(String methodName, long startTime, int task) {

            /* 先排序数据 */
            FlyArrayList list = methodDataMap.get(task).get(methodName);
            list.sort();

            /* QFS */
            int QFS = list.length;
            /* P99 */
            int P99 = list.get((int) Math.ceil(QFS * 0.99) - 1);
            /* P50 */
            int P50 = list.get((int) Math.ceil(QFS * 0.5) - 1);
            /* AVG */
            int AVG = (list.total + QFS - 1) / QFS;
            /* MAX */
            int MAX = list.get(QFS - 1);

            /* 将答案答案字符串加入映射 */
            this.addAnswer(methodName, startTime,
                    QFS + "," + P99 + "," + P50 + "," + AVG + "," + MAX);

            /* list 需要复用，将其重置 */
            list.reset();

        }

    }

    /**
     * 线程任务信息
     */
    static class TaskInfo {
        int left;       /* 读取左边界 */
        int right;      /* 读取右边界 */
        long currTime;  /* 当前读取时间戳 */
        int task;       /* 任务号 */
    }

    /**
     * 构造函数，完成初始化
     */
    public KcodeQuestion() {
        for (int task = 0; task < NR_THREADS; ++task) {
            infos[task] = new TaskInfo();
        }
    }


    /**
     * prepare() 方法用来接受输入数据集，数据集格式参考README.md
     *
     * @param inputStream 输入流
     */
    public void prepare(InputStream inputStream) throws IOException {
        /* 多线程读取，这里很有可能输入文件是 > 2G 的，所以我们可能分多次，但最多分两次，文件 < 4G */
        FileChannel fileChannel = ((FileInputStream) inputStream).getChannel();
        long fileSize = fileChannel.size();
        int mapOffset = 0;
        int mapSize = (fileSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) fileSize);

        /* 先进行 mmap */
        MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mapOffset, mapSize);

        /* 得到所有方法 */
        FlyAsciiConverter converter = new FlyAsciiConverter(buffer, 0);
        long currTime = converter.convertLong() / 1000; /* 初始化 currTime */
        long startTime;
        String methodName;
        int elapsedTime;
        converter.offset = 0;       /* 恢复偏移 */
        boolean findOut = false;
        int methodSize;
        while (!findOut) {
            /* 先进行转换 */
            startTime = converter.convertLong() / 1000;
            methodName = converter.convertString();
            elapsedTime = converter.convertInt();

            /* 如果得到的开始时间不等于当前正在读取的时间，计算 */
            if(startTime != currTime) {
                /* 将所有方法转换为数组，便于使用 */
                methodSize = MethodSet.size();
                methodArray = MethodSet.toArray(new String[methodSize]);

                /* 计算::对所有方法进行计算 */
                for(String m : methodArray) {
                    answer.compute(m, currTime, 0);
                }

                /* 更新当前读取时间 */
                currTime = startTime;
                findOut = true;            /* 已经找到所有方法 */
            }
            /* 加入到输入数据区域 */
            answer.addElapsedTime(methodName, elapsedTime, 0);
            MethodSet.add(methodName);
        }
        final int first_left = converter.offset;
        final long first_currTime = currTime;

        /* 多线程读取并要保持内存复用的最大的难点，我们要为每个线程分好左右边界。 */

        /* 调整整个 buffer 的右边界 */
        /* 检查右边界::找到 2G 前的一个 '\n' */
        mapSize--;
        while ( (buffer.get(mapSize)) != '\n' )
            mapSize--;
        /* 再找第二个 '\n' 作为处理的起始右边界，第一个 '\n' 可能会有问题，
         * 因为 2G 可能刚好处于 "\n123456"，这个时候 123456 不是一个完整
         * 的执行方法开始时间戳，下面的 convertLong 会发生越界。
         */
        mapSize--;
        while ( (buffer.get(mapSize)) != '\n' )
            mapSize--;
        mapSize++;      /* 要在 '\n' 之后 */


        converter.offset = mapSize;
        /* 现在，先得到这块区域的时间戳区域 */
        currTime = converter.convertLong() / 1000;
        final long next_currTime = currTime;    /* 如果一次处理不完，它就是下次处理第一个线程的 currTime */
        /* 一直往回找，找到不同于当前时间区域的地方，作为右边界 */
        backtrack(mapSize, buffer, converter, currTime);
        /* 这时候偏移指向我们想要的目标前面一条记录，我们跳过 */
        converter.skipOneLine();
        mapSize = converter.offset; /* 这个时候的偏移才是我们需要的 */
        /* 至此，我们找到了 buffer 的右边界 */



        /* 接下来，为第一个线程找右边界 */
        int perTaskSize = mapSize / NR_THREADS;
        infos[0].task = 0;
        infos[0].left = first_left;             /* 左边界，为什么不是 0？因为前面得到所有方法时已经算过一趟了，第一个线程没必要重复再算一次 */
        infos[0].currTime = first_currTime;
        int last_right = perTaskSize;           /* 右边界估略在这 */
        /* 检查右边界::找到前的一个 '\n' 作为处理的起始右边界 */
        last_right--;
        while ( (buffer.get(last_right)) != '\n' )
            last_right--;
        last_right++;
        converter.offset = last_right;
        /* 现在，先得到这块区域的时间戳区域 */
        currTime = converter.convertLong() / 1000;
        infos[1].currTime = currTime;
        /* 一直往回找，找到不同于当前时间区域的地方，作为右边界 */
        backtrack(last_right, buffer, converter, currTime);
        /* 这时候偏移指向我们想要的目标前面一条记录，我们跳过后续方法名称和耗时 */
        converter.skipOneLine();
        infos[0].right = last_right = converter.offset; /* 这个时候的偏移才是我们需要的 */


        /* 好的，为中间的线程调整左右边界 */
        middleTaskInfoInit(1, last_right, perTaskSize, buffer, converter);

        /* 最后，为最后一个线程调整左右边边界*/
        infos[NR_THREADS - 1].task = NR_THREADS - 1;
        infos[NR_THREADS - 1].left
                = infos[NR_THREADS - 2].right; /* 左边界即上一个线程的右边界 */
        infos[NR_THREADS - 1].right = mapSize; /* 右边界即 buffer 的右边界  */

        /* 好的，为每个线程参数开启一个线程来完成任务 */
        CountDownLatch latch = new CountDownLatch(NR_THREADS);
        Reader[] reader = new Reader[NR_THREADS];
        for(TaskInfo info : infos) {
            reader[info.task] = new Reader();
            reader[info.task].setData(info, buffer, latch);
            pool.execute(reader[info.task]);
        }

        /* 等待所有任务执行完毕 */
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        /* 如果文件 > 2G，可能需要第二次处理？ */
        if(fileSize > Integer.MAX_VALUE) {
            mapOffset += mapSize;
            mapSize = (int) (fileSize - mapSize);
            buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mapOffset, mapSize);
            converter.buffer = buffer;
            converter.offset = last_right = 0;
            perTaskSize = mapSize / NR_THREADS;

            /* 好的，前面线程调整左右边界，它们跟在上一次 2G 文件处理后 */
            infos[0].currTime = next_currTime;
            middleTaskInfoInit(0, last_right, perTaskSize, buffer, converter);


            /* 最后，为最后一个线程调整左右边边界*/
            infos[NR_THREADS - 1].task = NR_THREADS - 1;
            infos[NR_THREADS - 1].left
                    = infos[NR_THREADS - 2].right; /* 左边界即上一个线程的右边界 */
            infos[NR_THREADS - 1].right = mapSize; /* 右边界即 buffer 的右边界  */

            /* 好的，为每个线程参数开启一个线程来完成任务 */
            latch = new CountDownLatch(NR_THREADS);
            for(TaskInfo info : infos) {
                reader[info.task].setData(info, buffer, latch);
                pool.execute(reader[info.task]);
            }

            /* 等待所有任务执行完毕 */
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        pool.shutdown();        /* 线程池关闭 */
    }

    /**
     * getResult() 方法是由kcode评测系统调用，是评测程序正确性的一部分，请按照题目要求返回正确数据
     * 输入格式和输出格式参考 README.md
     *
     * 我们直接通过查答案映射表来获取记录的答案字符串
     *
     * @param timestamp 秒级时间戳
     * @param methodName 方法名称
     */
    public String getResult(Long timestamp, String methodName) {
        return answer.getAnswer(methodName, timestamp);
    }

    /**
     * 对处于中间的线程参数进行设置
     *
     * @param startTask 从哪个线程开始
     * @param last_right 上一个线程的右边界
     * @param perTaskSize 每个线程应该处理多大的缓冲区
     * @param buffer 读进来的文件缓冲区
     * @param converter 转换器
     */
    private void middleTaskInfoInit(int startTask, int last_right, int perTaskSize,
                                    MappedByteBuffer buffer, FlyAsciiConverter converter) {
        long currTime;
        for(int task = startTask; task < NR_THREADS - 1; ++task) {
            infos[task].task = task;
            infos[task].left = last_right;  /* 左边界为上一个线程的右边界 */
            /* 右边界同上要进行一系列的调整 */
            last_right += perTaskSize;      /* 右边界估略在这 */
            /* 检查右边界::找到前的一个 '\n' 作为处理的起始右边界 */
            last_right--;
            while ( (buffer.get(last_right)) != '\n' )
                last_right--;
            last_right++;
            converter.offset = last_right;
            /* 现在，先得到这块区域的时间戳区域 */
            currTime = converter.convertLong() / 1000;
            infos[task + 1].currTime = currTime;
            /* 一直往回找，找到不同于当前时间区域的地方，作为右边界 */
            backtrack(last_right, buffer, converter, currTime);
            /* 这时候偏移指向我们想要的目标前面一条记录，我们跳过后续方法名称和耗时 */
            converter.skipOneLine();
            infos[task].right = last_right = converter.offset;  /* 这个时候的偏移才是我们需要的 */
        }
    }

    /**
     * 一直往回找，跳过不同于当前时间区域的地方
     *
     * @param offset 从哪里的偏移开始
     * @param buffer 读进来的文件缓冲区
     * @param converter 转换器
     * @param currTime 当前时间戳
     */
    private void backtrack(int offset, MappedByteBuffer buffer, FlyAsciiConverter converter, long currTime) {
        long startTime;
        do {
            offset -= 2;       /* 跳过第一个 '\n' */
            while ((buffer.get(offset)) != '\n')
                offset--;
            offset++;
            converter.offset = offset;
            startTime = converter.convertLong() / 1000;
        } while (currTime == startTime);
    }

    /**
     * 读取者
     */
    static class Reader implements Runnable {
        private int left;
        private int right;
        private long currTime;
        private int task;
        private MappedByteBuffer buffer;
        private CountDownLatch latch;

        void setData(TaskInfo info,
                     MappedByteBuffer buffer, CountDownLatch latch) {
            this.left = info.left;
            this.right = info.right;
            this.task = info.task;
            this.currTime = info.currTime;
            this.buffer = buffer;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
//                System.out.println("任务 " + this.task + " 左右边界以及当前读取时间: ["
//                        + this.left
//                        + ", " + this.right
//                        + ", " + this.currTime + "]");

                long startTime;
                String methodName;
                int elapsedTime;
                FlyAsciiConverter converter = new FlyAsciiConverter(this.buffer, this.left);

                while ( converter.offset < this.right ) {
                    /* 先将数据转换 */
                    startTime = converter.convertLong() / 1000;
                    methodName = converter.convertString();
                    elapsedTime = converter.convertInt();

                    if(startTime != this.currTime) {
//                        System.out.println(this.task + "::" + startTime + "::" + this.currTime);
                        /* 计算::对所有方法进行计算 */
                        for(String m : methodArray) {
                            answer.compute(m, currTime, this.task);
                        }
                        /* 更新当前读取时间 */
                        this.currTime = startTime;
                    }
                    /* 加入到输入数据区域 */
                    answer.addElapsedTime(methodName, elapsedTime, this.task);
                }
                /* 最后的一个也要算，因为碰不到 startTime != this.currTime 了 */
                for(String m : methodArray) {
                    answer.compute(m, currTime, this.task);
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    /**
     * 用于字符串转换各种类型，现在我们只需要转整形和长整型
     */
    static class FlyAsciiConverter {
        MappedByteBuffer buffer;    /* 待转换的映射缓冲区 */
        int offset;                 /* 转换成一个类型后，新的偏移 */

        FlyAsciiConverter(MappedByteBuffer buffer, int offset) {
            this.buffer = buffer;
            this.offset = offset;
        }

        /**
         * 字符串转换为整型，我们不考虑负数
         *
         * @return 转换成功的整数
         */
        int convertInt() {
            int value = 0;
            /* 逐个把字符串的字符转换为数字 */
            while ( (buffer.get(offset)) >= '0' ) {
                value *= 10;
                value += buffer.get(offset) - '0';
                offset++;
            }

//            /* 对于 \r 则后面还有一个 \n，也需要跳过 */
//            if( (buffer.get(offset)) == '\r' )
//                offset++;
            offset++;       /* 跳过后面的 '\n' */
            return value;
        }

        /**
         * 字符串转换为长整型，我们不考虑负数
         *
         * @return 转换成功的长整数
         */
        long convertLong() {
            long value = 0;
            /* 逐个把字符串的字符转换为数字 */
            while ( (buffer.get(offset)) >= '0' ) {
                value *= 10;
                value += buffer.get(offset) - '0';
                offset++;
            }

            offset++;       /* 跳过后面的 ',' */
            return value;
        }

        /**
         * 字符串转字符串？更确切的来说是获取一个以 ',' 结尾的字符串，不包括 ','
         * @return 转换成功的字符串
         */
        String convertString() {
            StringBuilder ans = new StringBuilder();
            while ( (buffer.get(offset)) != ',' ) {
                ans.append((char)buffer.get(offset));
                offset++;
            }
            offset++;       /* 跳过后面的 ',' */
            return ans.toString();
        }

        /**
         * 跳过一行记录
         */
        void skipOneLine() {
            while ( (this.buffer.get(this.offset)) != '\n' ) {
                this.offset++;
            }
            this.offset++;
        }

    }

    /* 自己用数组管理数据不用库里的 */
    static class FlyArrayList {
        /* 本体 */
        private int[] array;

        /* 有多少元素？ */
        private int length;

        private int total;

        /* 构造函数 */
        FlyArrayList() {
            this.array = new int[3150];
        }

        /* 重置 */
        void reset() {
            this.length = this.total = 0;
        }

        /* 获取元素 */
        int get(int idx) {
            return array[idx];
        }

        /* 添加元素 */
        void add(int elem) {
            this.array[this.length++] = elem;
            this.total += elem;
        }

        /* 排序::从小到大 */
        void sort() {
            Arrays.sort(this.array, 0, this.length);
        }
    }

}
