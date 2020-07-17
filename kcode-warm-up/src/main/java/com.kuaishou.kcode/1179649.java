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
 *
 */
public class KcodeQuestion {

    /* 线程池：拒绝策略为 AbortPolic 策略，直接抛出异常 */
    private final int NR_THREADS = 8;                      /* 最大线程数量，线下 8C4G */
    //    private final static int NR_THREADS = 16;                     /* 最大线程数量，线上 16C4G */
    private final int TIME_OUT = 80;                       /* 线程超时时间(s) */
    private ExecutorService pool = new ThreadPoolExecutor(NR_THREADS, NR_THREADS, TIME_OUT,
            TimeUnit.SECONDS, new SynchronousQueue<>(), Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.AbortPolicy());
    private TaskInfo[] infos = new TaskInfo[NR_THREADS];


    /* answer map table */
    private final int MAX_METHOD_SIZE = 80;          /* 方法跨度 */
    private final int MAX_TIME_SIZE = 4300;          /* 时间跨度 */
    private Answer answer = new Answer();      /* 答案管理对象 */
    private String[][] answerArray = new String[MAX_TIME_SIZE][MAX_METHOD_SIZE + 1];
    private int originTime;                          /* 第一条记录的时间 */

    /* 数据区域 */
    private HashSet<String> methodSet = new HashSet<>(MAX_METHOD_SIZE);
    private String[] methodArray = null;
//    private long t1;
//    private long t2;
//    private long len = 0L;

    /**
     * 答案类，管理答案
     */
    class Answer {
        private final int MAX_LIST_SIZE = 3151;
        private final int LIST_LENGTH =  MAX_LIST_SIZE - 1;     /* 作为数组长度 */
        private final int LIST_TOTAL = MAX_LIST_SIZE - 2;       /* 作为数据的总和 */
        private Map<String, int[]>[] methodDataMapArray =
                new Map[NR_THREADS];                            /* 方法的数据映射 */


        Answer() {
            for(int task = 0; task < NR_THREADS; ++task) {
                methodDataMapArray[task] = new HashMap<>(MAX_METHOD_SIZE);
            }
        }

        /**
         * 添加一个方法耗时
         *
         * @param methodName 执行方法
         * @param elapsedTime 耗时
         */
        void addElapsedTime(final String methodName, final int elapsedTime, final int task) {
            int[] data = methodDataMapArray[task].computeIfAbsent(methodName, k -> new int[MAX_LIST_SIZE]);
            data[data[LIST_LENGTH]++] = elapsedTime;
            data[LIST_TOTAL] += elapsedTime;
        }

        private int findMid(int[] array, int left, int right, final int k) {
            int pl = left, pr = right;
            int pivot = array[pl];
            while (pl < pr) {
                while (array[pr] >= pivot && pl < pr) {
                    pr--;
                }
                array[pl] = array[pr];
                while (array[pl] <= pivot && pl < pr) {
                    pl++;
                }
                array[pr] = array[pl];
            }
            array[pl] = pivot;
            int m = pl - left + 1;          /* mid */
            if(m > k) return findMid(array, left, pl - 1, k);
            else if(m < k) return findMid(array, pl + 1, right, k - m);
            return array[pl];               /* m == k */
        }

        /**
         * 计算结果
         * @param methodName 执行方法
         * @param startTime 方法开始执行时间戳，秒级
         */
        void compute(final String methodName, final int startTime, final int task) {

            int[] array = methodDataMapArray[task].get(methodName);
            int MAX, P99, P50, i, j, t, k;
            double AVG;
            /* QFS */
            int QFS = array[LIST_LENGTH];
            /* P50 */
            P50 = findMid(array, 0, QFS - 1, (QFS + 1) / 2);
            /* P99 */
            for (i = 0; i < QFS / 100 + 1; ++i) {
                k = i;
                for (j = i + 1; j < QFS; ++j) {
                    if (array[j] > array[k]) {
                        k = j;
                    }
                }
                if (i != k) {   /* swap */
                    t = array[i];
                    array[i] = array[k];
                    array[k] = t;
                }
            }
            P99 = array[QFS / 100];
            /* AVG */
            AVG = Math.ceil((double) array[LIST_TOTAL] / QFS);
            /* MAX */
            MAX = array[0];

            /* 将答案答案字符串加入映射 */
            int methodNameLength = methodName.length();
            answerArray[startTime - originTime]
                    [(methodNameLength - 5) * 10 + methodName.toCharArray()[methodNameLength - 1] - '0']
                    = (QFS + "," + P99 + "," + P50 + "," + (int) AVG + "," + MAX);

            /* 数组需要复用，将其长度和总和重置 */
            array[LIST_LENGTH] = array[LIST_TOTAL] = 0;

        }

    }

    /**
     * 线程任务信息
     */
    private class TaskInfo {
        int left;       /* 读取左边界 */
        int right;      /* 读取右边界 */
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
//        t1 = System.currentTimeMillis();
        /* 多线程读取，这里很有可能输入文件是 > 2G 的，所以我们可能分多次，但最多分两次，文件 < 4G */
        FileChannel fileChannel = ((FileInputStream) inputStream).getChannel();
        final long fileSize = fileChannel.size();
        int mapOffset = 0;
        int mapSize = (fileSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) fileSize);

        /* 先进行 mmap */
        MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mapOffset, mapSize);

        /* 得到所有方法 */
        FlyAsciiConverter converter = new FlyAsciiConverter(buffer, 0);
        int currTime = originTime = converter.currTime; /* 初始化 currTime */
        int startTime;
        String methodName;
        int elapsedTime;
        converter.offset = 0;       /* 恢复偏移 */
        boolean findOut = false;
        int methodSize;
        while (!findOut) {
            /* 先进行转换 */
            startTime = converter.bufferConvertTimeStamp();
            methodName = converter.convertString();
            elapsedTime = converter.convertInt();

            /* 如果得到的开始时间不等于当前正在读取的时间，计算 */
            if(startTime != currTime) {
                /* 将所有方法转换为数组，便于使用 */
                methodSize = methodSet.size();
                methodArray = methodSet.toArray(new String[methodSize]);

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
            methodSet.add(methodName);
        }
        final int first_left = converter.offset;

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
        currTime = converter.convertTimeStamp();
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
        int last_right = perTaskSize;           /* 右边界估略在这 */
        /* 检查右边界::找到前的一个 '\n' 作为处理的起始右边界 */
        last_right--;
        while ( (buffer.get(last_right)) != '\n' )
            last_right--;
        last_right++;
        converter.offset = last_right;
        /* 现在，先得到这块区域的时间戳区域 */
        currTime = converter.convertTimeStamp();
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

        /* 线程池关闭 */
        pool.shutdown();
//        System.gc();
    }

    /**
     * getResult() 方法是由kcode评测系统调用，是评测程序正确性的一部分，请按照题目要求返回正确数据
     * 输入格式和输出格式参考 README.md
     *
     * 我们直接通过查答案表来获取记录的答案字符串
     *
     * @param timestamp 秒级时间戳
     * @param methodName 方法名称
     */
    public String getResult(Long timestamp, String methodName) {
//        if (len == 0L) {
//            t2 = System.currentTimeMillis();
//            t1 = t2 - t1;
//        } else if (len > 170000000) {
//            t2 = System.currentTimeMillis() - t2;
//            throw new Exception(t1 + "::" + t2);
//        }
//        len++;

        int methodNameLength = methodName.length();
        return answerArray[timestamp.intValue() - originTime]
                [(methodNameLength - 5) * 10 + methodName.toCharArray()[methodNameLength - 1] - '0'];
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
    private void middleTaskInfoInit(final int startTask, int last_right, final int perTaskSize,
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
            currTime = converter.convertTimeStamp();
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
    private void backtrack(int offset, MappedByteBuffer buffer, FlyAsciiConverter converter, final long currTime) {
        long startTime;
        do {
            offset -= 2;       /* 跳过第一个 '\n' */
            while ((buffer.get(offset)) != '\n')
                offset--;
            offset++;
            converter.offset = offset;
            startTime = converter.convertTimeStamp();
        } while (currTime == startTime);
    }

    /**
     * 读取者
     */
    private class Reader implements Runnable {
        private int left;
        private int right;
        private int task;
        private MappedByteBuffer buffer;
        private CountDownLatch latch;

        void setData(TaskInfo info,
                     MappedByteBuffer buffer, CountDownLatch latch) {
            this.left = info.left;
            this.right = info.right;
            this.task = info.task;
            this.buffer = buffer;
            this.latch = latch;
        }

        @Override
        public void run() {
//            System.out.println("任务 " + this.task + " 左右边界以及当前读取时间: ["
//                + this.left
//                + ", " + this.right
//                + ", " + this.currTime + "]");

            int startTime;
            String methodName;
            int elapsedTime;
            FlyAsciiConverter converter = new FlyAsciiConverter(this.buffer, this.left);
            int currTime = converter.currTime;  /* 当前读取时间戳 */

            try {

                while ( converter.offset < this.right ) {
                    /* 先将数据转换 */
                    startTime = converter.bufferConvertTimeStamp();
                    methodName = converter.convertString();
                    elapsedTime = converter.convertInt();

                    if(startTime != currTime) {
                        /* 计算::对所有方法进行计算 */
                        for(String m : methodArray) {
                            answer.compute(m, currTime, this.task);
                        }
                        /* 更新当前读取时间 */
                        currTime = startTime;
                    }
                    /* 加入到输入数据区域 */
                    answer.addElapsedTime(methodName, elapsedTime, this.task);
                }
                /* 最后的一个也要算，因为碰不到 startTime != currTime 了 */
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
    private class FlyAsciiConverter {
        private MappedByteBuffer buffer;    /* 待转换的映射缓冲区 */
        private int offset;                 /* 转换成一个类型后，新的偏移 */
        private int currTime;               /* 当前读取的时间戳 */
        private byte currTimeFeature;       /* 当前读取的时间戳的特征 */
//        private char[] stringBuffer;
        private byte currChar;

        FlyAsciiConverter(MappedByteBuffer buffer, int offset) {
            this.buffer = buffer;
            this.offset = offset;
//            this.stringBuffer = new char[16];
            /* 得到当前时间戳的特征 */
            this.currTimeFeature = this.buffer.get(offset + 9);
            this.currTime = this.convertTimeStamp();
            this.offset = offset;   /* 恢复 */
        }

        /**
         * 字符串转换为整型，我们不考虑负数
         *
         * @return 转换成功的整数
         */
        int convertInt() {
            int value = 0;
            /* 逐个把字符串的字符转换为数字 */
            while ( (currChar = buffer.get(offset)) >= '0' ) {
                value *= 10;
                value += currChar - '0';
                offset++;
            }

            offset++;       /* 跳过后面的 '\n' */
            return value;
        }

        /**
         * 字符串转换为一个秒级时间戳
         *
         * @return 秒级时间戳
         */
        int convertTimeStamp() {
            this.currTime = 0;
            for(int i = 0; i < 10; ++i) {
                this.currTime = this.currTime * 10 + buffer.get(offset++) - '0';
            }
            this.currTimeFeature = this.buffer.get(offset - 1);

            offset += 4;       /* 跳过后面的剩余毫秒级和 ',' */
            return this.currTime;
        }

        /**
         * 带缓存的秒级时间戳转换
         *
         * @return 秒级时间戳
         */
        int bufferConvertTimeStamp() {
            /* 如果时间未改变，直接返回以前的时间，否则 +1s */
            this.offset += 14;  /* 更新偏移 */
            byte timeFeature = this.buffer.get(this.offset - 5);
            if(this.currTimeFeature == timeFeature) {
                return this.currTime;
            } else {
                /* 时间改变，更新当前特征 */
                this.currTimeFeature = timeFeature;
                return ++this.currTime;
            }
        }

        /**
         * 字符串转字符串？更确切的来说是获取一个以 ',' 结尾的字符串，不包括 ','
         * @return 转换成功的字符串
         */
        String convertString() {
            StringBuilder ans = new StringBuilder();
            while ( (currChar = buffer.get(offset)) != ',' ) {
                ans.append((char)currChar);
                offset++;
            }
            offset++;       /* 跳过后面的 ',' */
            return ans.toString();

            /* 不知道为什么，这样反而影响查询时间？搞不懂 */
//            int off = 0;
//            while ( (buffer.get(offset)) != ',' ) {
//                stringBuffer[off++] = (char) buffer.get(offset++);
//            }
//            offset++;       /* 跳过后面的 ',' */
//            return new String(stringBuffer, 0, off);
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

}
