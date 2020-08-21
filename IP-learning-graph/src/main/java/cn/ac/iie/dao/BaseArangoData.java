package cn.ac.iie.dao;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.service.ingestion.ReadHistoryArangoData;
import cn.ac.iie.utils.ArangoDBConnect;
import cn.ac.iie.utils.ExecutorThreadPool;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * 获取arangoDB历史数据
 */
public class BaseArangoData {
    private static final Logger LOG = LoggerFactory.getLogger(BaseArangoData.class);

    static ConcurrentHashMap<Integer,ConcurrentHashMap<String, BaseDocument>> historyVertexFqdnMap = new ConcurrentHashMap<>();
    static ConcurrentHashMap<Integer,ConcurrentHashMap<String, BaseDocument>> historyVertexIpMap = new ConcurrentHashMap<>();
    static ConcurrentHashMap<Integer,ConcurrentHashMap<String, BaseDocument>> historyVertexSubscriberMap = new ConcurrentHashMap<>();
    static ConcurrentHashMap<Integer,ConcurrentHashMap<String, BaseEdgeDocument>> historyRelationFqdnAddressIpMap = new ConcurrentHashMap<>();
    static ConcurrentHashMap<Integer,ConcurrentHashMap<String, BaseEdgeDocument>> historyRelationIpVisitFqdnMap = new ConcurrentHashMap<>();
    static ConcurrentHashMap<Integer,ConcurrentHashMap<String, BaseEdgeDocument>> historyRelationFqdnSameFqdnMap = new ConcurrentHashMap<>();
    static ConcurrentHashMap<Integer,ConcurrentHashMap<String, BaseEdgeDocument>> historyRelationSubsciberLocateIpMap = new ConcurrentHashMap<>();

    private static ArangoDBConnect arangoDBConnect = ArangoDBConnect.getInstance();

    private ExecutorThreadPool threadPool = ExecutorThreadPool.getInstance();

    <T extends BaseDocument> void readHistoryData(String table,
                                                  ConcurrentHashMap<Integer,ConcurrentHashMap<String, T>> map,
                                                  Class<T> type) {
        try {
            LOG.info("开始更新"+table);
            long start = System.currentTimeMillis();
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++){
                map.put(i,new ConcurrentHashMap<>());
            }
            CountDownLatch countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
//            long[] timeRange = getTimeRange(table);
            Long countTotal = getCountTotal(table);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
//                String sql = getQuerySql(timeRange, i, table);
                String sql = getQuerySql(countTotal, i, table);
                ReadHistoryArangoData<T> readHistoryArangoData =
                        new ReadHistoryArangoData<>(arangoDBConnect, sql, map,type,table,countDownLatch);
                threadPool.executor(readHistoryArangoData);
            }
            countDownLatch.await();
            long last = System.currentTimeMillis();
            LOG.info("读取"+table+" arangoDB 共耗时："+(last-start));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public <T extends BaseDocument> ConcurrentHashMap<Integer,ConcurrentHashMap<String, T>> readHistoryData(String table, Class<T> type){
        ConcurrentHashMap<Integer,ConcurrentHashMap<String, T>> map = new ConcurrentHashMap<>();
        try {
            LOG.info("开始更新"+table);
            long start = System.currentTimeMillis();
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++){
                map.put(i,new ConcurrentHashMap<>());
            }
            CountDownLatch countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            Long countTotal = getCountTotal(table);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                String sql = getQuerySql(countTotal, i, table);
                ReadHistoryArangoData<T> readHistoryArangoData =
                        new ReadHistoryArangoData<>(arangoDBConnect, sql, map,type,table,countDownLatch);
                threadPool.executor(readHistoryArangoData);
            }
            countDownLatch.await();
            long last = System.currentTimeMillis();
            LOG.info("读取"+table+" arangoDB 共耗时："+(last-start));
        }catch (Exception e){
            e.printStackTrace();
            LOG.error("读取历史数据失败 "+e.toString());
        }
        return map;
    }

    private long[] getTimeRange(String table){
        long minTime = 0L;
        long maxTime = 0L;
        long startTime = System.currentTimeMillis();
        String sql = "LET doc = (FOR doc IN "+table+" RETURN doc) return {max_time:MAX(doc[*].FIRST_FOUND_TIME),min_time:MIN(doc[*].FIRST_FOUND_TIME)}";
        ArangoCursor<BaseDocument> timeDoc = arangoDBConnect.executorQuery(sql, BaseDocument.class);
        try {
            if (timeDoc != null){
                while (timeDoc.hasNext()) {
                    BaseDocument doc = timeDoc.next();
                    maxTime = Long.parseLong(doc.getAttribute("max_time").toString()) + ApplicationConfig.THREAD_POOL_NUMBER;
                    minTime = Long.parseLong(doc.getAttribute("min_time").toString());
                }
                long lastTime = System.currentTimeMillis();
                LOG.info(sql+"\n查询最大最小时间用时：" + (lastTime - startTime));
            }else {
                LOG.warn("获取ArangoDb时间范围为空");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return new long[]{minTime, maxTime};

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
        long sepNum = cnt / ApplicationConfig.THREAD_POOL_NUMBER + 1;
        long offsetNum = threadNumber * sepNum;
        return "FOR doc IN " + table + " limit "+offsetNum+","+sepNum+" RETURN doc";
    }


    private String getQuerySql(long[] timeRange,int threadNumber,String table){
        long minTime = timeRange[0];
        long maxTime = timeRange[1];
        long diffTime = (maxTime - minTime) / ApplicationConfig.THREAD_POOL_NUMBER;
        long maxThreadTime = minTime + (threadNumber + 1)* diffTime;
        long minThreadTime = minTime + threadNumber * diffTime;
        return "FOR doc IN "+table+" filter doc.FIRST_FOUND_TIME >= "+minThreadTime+" and doc.FIRST_FOUND_TIME <= "+maxThreadTime+" " + ApplicationConfig.ARANGODB_READ_LIMIT + " RETURN doc";
    }

}
