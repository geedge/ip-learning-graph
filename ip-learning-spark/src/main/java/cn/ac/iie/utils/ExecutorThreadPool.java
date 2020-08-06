package cn.ac.iie.utils;

import cn.ac.iie.config.ApplicationConfig;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * 线程池管理
 * @author wlh
 */
public class ExecutorThreadPool {
    private static ExecutorService pool = null ;
    private static ExecutorThreadPool poolExecutor = null;

    static {
        getThreadPool();
    }

    private static void getThreadPool(){
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("iplearning-application-pool-%d").build();

        //Common Thread Pool
        pool = new ThreadPoolExecutor(ApplicationConfig.THREAD_POOL_NUMBER(), ApplicationConfig.THREAD_POOL_NUMBER()*2,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());

//        pool = Executors.newFixedThreadPool(ApplicationConfig.THREAD_POOL_NUMBER);
    }

    public static ExecutorThreadPool getInstance(){
        if (null == poolExecutor){
            poolExecutor = new ExecutorThreadPool();
        }
        return poolExecutor;
    }

    public void executor(Runnable command){
        pool.execute(command);
    }

    public void shutdown(){
        pool.shutdown();
    }


}
