package cn.ac.iie.dao;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.etl.fqdn2ip.ArangoEFqdnAddressIpToMap;
import cn.ac.iie.etl.ip2fqdn.ArangoEIpVisitFqdnToMap;
import cn.ac.iie.etl.fqdn.ArangoVFqdnToMap;
import cn.ac.iie.etl.ip.ArangoVIpToMap;
import cn.ac.iie.utils.ArangoDBConnect;
import cn.ac.iie.utils.ExecutorThreadPool;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class BaseArangoData {
    private static final Logger LOG = LoggerFactory.getLogger(BaseArangoData.class);

    public static ConcurrentHashMap<String, BaseDocument> v_Fqdn_Map = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, BaseDocument> v_Ip_Map = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, BaseEdgeDocument> e_Fqdn_Address_Ip_Map = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, BaseEdgeDocument> e_Ip_Visit_Fqdn_Map = new ConcurrentHashMap<>();

    private static final ArangoDBConnect arangoDBConnect = ArangoDBConnect.getInstance();

    private static final ExecutorThreadPool threadPool = ExecutorThreadPool.getInstance();

    public static void BaseVFqdnDataMap() {
        String sql = "LET FQDN = (FOR doc IN FQDN RETURN doc) return {max_time:MAX(FQDN[*].FIRST_FOUND_TIME),min_time:MIN(FQDN[*].FIRST_FOUND_TIME)}";
        long[] timeLimit = getTimeLimit(sql);
        for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
            ArangoVFqdnToMap ArangoVFqdnToMap = new ArangoVFqdnToMap(arangoDBConnect, timeLimit[0], timeLimit[2],i);
            threadPool.executor(ArangoVFqdnToMap);
        }
    }

    public static void BaseVIpDataMap() {
        String sql = "LET IP = (FOR doc IN IP RETURN doc) return {max_time:MAX(IP[*].FIRST_FOUND_TIME),min_time:MIN(IP[*].FIRST_FOUND_TIME)}";
        long[] timeLimit = getTimeLimit(sql);
        for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
            ArangoVIpToMap arangoVIpToMap = new ArangoVIpToMap(arangoDBConnect, timeLimit[0], timeLimit[2],i);
            threadPool.executor(arangoVIpToMap);
        }
    }

    public static void BaseEFqdnAddressIpDataMap(){
        String sql = "LET e = (FOR doc IN R_LOCATE_FQDN2IP RETURN doc) return {max_time:MAX(e[*].FIRST_FOUND_TIME),min_time:MIN(e[*].FIRST_FOUND_TIME)}";
        long[] timeLimit = getTimeLimit(sql);
        for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++){
            ArangoEFqdnAddressIpToMap arangoEFqdnAddressIpToMap = new ArangoEFqdnAddressIpToMap(arangoDBConnect, timeLimit[0], timeLimit[2], i);
            threadPool.executor(arangoEFqdnAddressIpToMap);
        }
    }

    public static void BaseEIpVisitFqdnDataMap(){
        String sql = "LET e = (FOR doc IN R_VISIT_IP2FQDN RETURN doc) return {max_time:MAX(e[*].FIRST_FOUND_TIME),min_time:MIN(e[*].FIRST_FOUND_TIME)}";
        long[] timeLimit = getTimeLimit(sql);
        for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++){
            ArangoEIpVisitFqdnToMap arangoEIpVisitFqdnToMap = new ArangoEIpVisitFqdnToMap(arangoDBConnect, timeLimit[0], timeLimit[2], i);
            threadPool.executor(arangoEIpVisitFqdnToMap);
        }
    }

    private static long[] getTimeLimit(String sql) {
        long minTime = 0L;
        long maxTime = 0L;
        long diffTime = 0L;
        long startTime = System.currentTimeMillis();
//        LOG.info(sql);
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
                diffTime = (maxTime - minTime) / ApplicationConfig.THREAD_POOL_NUMBER;
            }else {
                LOG.warn("获取ArangoDb时间范围为空");
            }
        }catch (Exception e){
            LOG.error(e.toString());
        }
        return new long[]{minTime, maxTime, diffTime};
    }

}
