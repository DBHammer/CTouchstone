package ecnu.db.utils.threadpool;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author wangqingshuai
 */
public class DataGenerationThreadPool {
    private static final int CORE_NUM = Runtime.getRuntime().availableProcessors();
    private static final int MAX_POOL_SIZE = CORE_NUM;
    private static final int KEEP_ALIVE_TIME = 10000;
    /**
     * 线程池
     */
    private static final ThreadPoolExecutor THREAD_POOL_EXECUTOR = new ThreadPoolExecutor(
            CORE_NUM,
            MAX_POOL_SIZE,
            KEEP_ALIVE_TIME,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), new TouchstoneThreadFactory("DataGeneration"));


    public static ThreadPoolExecutor getThreadPoolExecutor() {
        return THREAD_POOL_EXECUTOR;
    }

    public static void closeThreadPool() {
        THREAD_POOL_EXECUTOR.shutdown();
    }
}
