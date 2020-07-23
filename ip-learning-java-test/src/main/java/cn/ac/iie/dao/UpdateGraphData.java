package cn.ac.iie.dao;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.service.read.ReadClickhouseData;
import cn.ac.iie.service.update.Document;
import cn.ac.iie.service.update.relationship.LocateFqdn2Ip;
import cn.ac.iie.service.update.relationship.LocateSubscriber2Ip;
import cn.ac.iie.service.update.relationship.SameFqdn2Fqdn;
import cn.ac.iie.service.update.relationship.VisitIp2Fqdn;
import cn.ac.iie.service.update.vertex.Fqdn;
import cn.ac.iie.service.update.vertex.Ip;
import cn.ac.iie.service.update.vertex.Subscriber;
import cn.ac.iie.utils.ArangoDBConnect;
import cn.ac.iie.utils.ExecutorThreadPool;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.Supplier;

import static cn.ac.iie.dao.BaseArangoData.*;
import static cn.ac.iie.dao.BaseClickhouseData.*;

/**
 * 更新图数据库业务类
 * @author wlh
 */
public class UpdateGraphData {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateGraphData.class);
    private static ExecutorThreadPool pool = ExecutorThreadPool.getInstance();
    private static ArangoDBConnect arangoManger = ArangoDBConnect.getInstance();

    private static BaseArangoData baseArangoData = new BaseArangoData();
    private static BaseClickhouseData baseClickhouseData = new BaseClickhouseData();

    public void updateArango(){
        long start = System.currentTimeMillis();
        try {
            updateDocument(newVertexFqdnMap, historyVertexFqdnMap, "FQDN",
                    Fqdn.class,BaseDocument.class,
                    ReadClickhouseData::getVertexFqdnSql,ReadClickhouseData::getVertexFqdnDocument);

            updateDocument(newVertexIpMap,historyVertexIpMap,"IP",
                    Ip.class,BaseDocument.class,
                    ReadClickhouseData::getVertexIpSql,ReadClickhouseData::getVertexIpDocument);

            updateDocument(newRelationFqdnAddressIpMap,historyRelationFqdnAddressIpMap,"R_LOCATE_FQDN2IP",
                    LocateFqdn2Ip.class,BaseEdgeDocument.class,
                    ReadClickhouseData::getRelationshipFqdnAddressIpSql,ReadClickhouseData::getRelationFqdnAddressIpDocument);

            updateDocument(newRelationIpVisitFqdnMap,historyRelationIpVisitFqdnMap,"R_VISIT_IP2FQDN",
                    VisitIp2Fqdn.class,BaseEdgeDocument.class,
                    ReadClickhouseData::getRelationshipIpVisitFqdnSql,ReadClickhouseData::getRelationIpVisitFqdnDocument);

            updateDocument(newRelationFqdnSameFqdnMap,historyRelationFqdnSameFqdnMap,"R_SAME_ORIGIN_FQDN2FQDN",
                    SameFqdn2Fqdn.class,BaseEdgeDocument.class,
                    ReadClickhouseData::getRelationshipFqdnSameFqdnSql,ReadClickhouseData::getRelationshipFqdnSameFqdnDocument);


            long last = System.currentTimeMillis();
            LOG.info("更新图数据库时间共计："+(last - start));
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            arangoManger.clean();
            pool.shutdown();
        }
    }

    private <T extends BaseDocument> void updateDocument(HashMap<Integer, HashMap<String, ArrayList<T>>> newMap,
                                                         ConcurrentHashMap<Integer, ConcurrentHashMap<String, T>> historyMap,
                                                         String collection,
                                                         Class<? extends Document<T>> taskType,
                                                         Class<T> docmentType,
                                                         Supplier<String> getSqlSupplier,
                                                         Function<ResultSet,T> formatResultFunc) {
        try {

            baseArangoData.readHistoryData(collection,historyMap,docmentType);
            LOG.info(collection+" 读取clickhouse,封装结果集");
            baseClickhouseData.baseDocumentFromClickhouse(newMap, getSqlSupplier,formatResultFunc);

            LOG.info(collection+" 开始更新");
            long start = System.currentTimeMillis();
            CountDownLatch countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++){
                HashMap<String, ArrayList<T>> tmpNewMap = newMap.get(i);
                ConcurrentHashMap<String, T> tmpHisMap = historyMap.get(i);
                Constructor constructor = taskType.getConstructor(
                        HashMap.class,
                        ArangoDBConnect.class,
                        String.class,
                        ConcurrentHashMap.class,
                        CountDownLatch.class);
                Document<T> docTask = (Document<T>)constructor.newInstance(tmpNewMap, arangoManger, collection, tmpHisMap, countDownLatch);
                pool.executor(docTask);
            }
            countDownLatch.await();
            long last = System.currentTimeMillis();
            LOG.info(collection+" 更新完毕,共耗时："+(last-start));
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            newMap.clear();
            historyMap.clear();
        }
    }


}
