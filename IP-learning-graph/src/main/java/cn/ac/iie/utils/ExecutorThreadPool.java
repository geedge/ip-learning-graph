package cn.ac.iie.utils;

import cn.ac.iie.config.ApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExecutorThreadPool {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorThreadPool.class);
    private static ExecutorService pool = null ;
    private static ExecutorThreadPool poolExecutor = null;

    static {
        getThreadPool();
    }

    private static void getThreadPool(){
        pool = Executors.newFixedThreadPool(ApplicationConfig.THREAD_POOL_NUMBER);
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

    public void awaitThreadTask(){
        try {
            while (!pool.awaitTermination(ApplicationConfig.THREAD_AWAIT_TERMINATION_TIME, TimeUnit.SECONDS)) {
                LOG.warn("线程池没有关闭");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void shutdown(){
        pool.shutdown();
    }

    @Deprecated
    public static Long getThreadNumber(){
        String name = Thread.currentThread().getName();
        String[] split = name.split("-");
        return Long.parseLong(split[3]);
    }



}
