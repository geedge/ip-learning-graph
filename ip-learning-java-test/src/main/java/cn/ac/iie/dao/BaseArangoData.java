package cn.ac.iie.dao;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.service.read.ReadHistoryArangoData;
import cn.ac.iie.utils.ArangoDBConnect;
import cn.ac.iie.utils.ClickhouseConnect;
import cn.ac.iie.utils.ExecutorThreadPool;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 获取arangoDB历史数据
 */
public class BaseArangoData {
    private static final Logger LOG = LoggerFactory.getLogger(BaseArangoData.class);

    public static ConcurrentHashMap<String, BaseDocument> v_Fqdn_Map = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, BaseDocument> v_Ip_Map = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, BaseDocument> v_Subscriber_Map = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, BaseEdgeDocument> e_Fqdn_Address_Ip_Map = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, BaseEdgeDocument> e_Ip_Visit_Fqdn_Map = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, BaseEdgeDocument> e_Subsciber_Locate_Ip_Map = new ConcurrentHashMap<>();

    private static ArangoDBConnect arangoDBConnect = ArangoDBConnect.getInstance();

    private static ExecutorThreadPool threadPool = ExecutorThreadPool.getInstance();

    public void baseDocumentDataMap(){
        long startA = System.currentTimeMillis();
        readHistoryData("FQDN", v_Fqdn_Map,BaseDocument.class);
        readHistoryData("IP", v_Ip_Map,BaseDocument.class);
        readHistoryData("SUBSCRIBER",v_Subscriber_Map,BaseDocument.class);
//        readHistoryData("R_LOCATE_FQDN2IP", e_Fqdn_Address_Ip_Map);
//        readHistoryData("R_VISIT_IP2FQDN", e_Ip_Visit_Fqdn_Map);
//        readHistoryData("R_LOCATE_SUBSCRIBER2IP",e_Subsciber_Locate_Ip_Map);
        threadPool.shutdown();
        threadPool.awaitThreadTask();
        LOG.info("v_Fqdn_Map大小："+v_Fqdn_Map.size());
        LOG.info("v_Ip_Map大小："+v_Ip_Map.size());
        LOG.info("v_Subscriber_Map大小："+v_Subscriber_Map.size());
        LOG.info("e_Fqdn_Address_Ip_Map大小："+e_Fqdn_Address_Ip_Map.size());
        LOG.info("e_Ip_Visit_Fqdn_Map大小："+e_Ip_Visit_Fqdn_Map.size());
        LOG.info("e_Subsciber_Locate_Ip_Map大小："+e_Subsciber_Locate_Ip_Map.size());
        long lastA = System.currentTimeMillis();
        LOG.info("读取ArangoDb时间："+(lastA - startA));
    }

    public static void main(String[] args) {
        new BaseArangoData().readHistoryData("IP", v_Ip_Map,BaseDocument.class);
        threadPool.shutdown();
        threadPool.awaitThreadTask();
        ArrayList<BaseEdgeDocument> baseEdgeDocuments = new ArrayList<>();
        Enumeration<String> keys = v_Ip_Map.keys();
        arangoDBConnect.overwrite(baseEdgeDocuments,"IP");
        arangoDBConnect.clean();

    }

    private <T extends BaseDocument> void readHistoryData(String table, ConcurrentHashMap<String, T> map, Class<T> type){
        try {
            long[] timeRange = getTimeRange(table);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                String sql = getQuerySql(timeRange, i, table);
                ReadHistoryArangoData<T> readHistoryArangoData = new ReadHistoryArangoData<>(arangoDBConnect, sql, map,type);
                threadPool.executor(readHistoryArangoData);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
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

    private String getQuerySql(long[] timeRange,int threadNumber,String table){
        long minTime = timeRange[0];
        long maxTime = timeRange[1];
        long diffTime = (maxTime - minTime) / ApplicationConfig.THREAD_POOL_NUMBER;
        long maxThreadTime = minTime + (threadNumber + 1)* diffTime;
        long minThreadTime = minTime + threadNumber * diffTime;
        return "FOR doc IN "+table+" filter doc.FIRST_FOUND_TIME >= "+minThreadTime+" and doc.FIRST_FOUND_TIME <= "+maxThreadTime+" RETURN doc";
    }

}
