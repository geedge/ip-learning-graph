package cn.ac.iie.dao;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.service.read.ReadHistoryArangoData;
import cn.ac.iie.utils.ArangoDBConnect;
import cn.ac.iie.utils.ExecutorThreadPool;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * 获取arangoDB历史数据
 *
 * @author wlh
 */
public class BaseArangoData {
    private static final Logger LOG = LoggerFactory.getLogger(BaseArangoData.class);
    private static ArangoDBConnect arangoDBConnect = ArangoDBConnect.getInstance();

    private ExecutorThreadPool threadPool = ExecutorThreadPool.getInstance();

    public <T extends BaseDocument> void readHistoryData(String table,
                                                  ConcurrentHashMap<Integer, ConcurrentHashMap<String, T>> historyMap,
                                                  Class<T> type) {
        try {
            LOG.warn("开始更新" + table);
            long start = System.currentTimeMillis();
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER(); i++) {
                historyMap.put(i, new ConcurrentHashMap<>());
            }
            CountDownLatch countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER());
//            long[] timeRange = getTimeRange(table);
            Long countTotal = getCountTotal(table);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER(); i++) {
//                String sql = getQuerySql(timeRange, i, table);
                String sql = getQuerySql(countTotal, i, table);
                ReadHistoryArangoData<T> readHistoryArangoData = new ReadHistoryArangoData<>(arangoDBConnect, sql, historyMap, type, table, countDownLatch);
                threadPool.executor(readHistoryArangoData);
            }
            countDownLatch.await();
            long last = System.currentTimeMillis();
            LOG.warn("读取" + table + " arangoDB 共耗时：" + (last - start));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Long getCountTotal(String table){
        long start = System.currentTimeMillis();
        Long cnt = 0L;
        String sql = "RETURN LENGTH("+table+")";
        try {
            ArangoCursor<Long> longs = arangoDBConnect.executorQuery(sql, Long.class);
            while (longs.hasNext()){
                cnt = longs.next();
            }
        }catch (Exception e){
            LOG.error(sql +"执行异常");
        }
        long last = System.currentTimeMillis();
        LOG.info(sql+" 结果："+cnt+" 执行时间："+(last-start));
        return cnt;
    }

    private String getQuerySql(Long cnt,int threadNumber, String table){
        long sepNum = cnt / ApplicationConfig.THREAD_POOL_NUMBER() + 1;
        long offsetNum = threadNumber * sepNum;
        return "FOR doc IN " + table + " limit "+offsetNum+","+sepNum+" RETURN doc";
    }

}
