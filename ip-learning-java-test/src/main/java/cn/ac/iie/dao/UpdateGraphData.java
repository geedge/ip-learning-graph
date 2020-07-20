package cn.ac.iie.dao;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.service.update.Document;
import cn.ac.iie.service.update.relationship.LocateFqdn2Ip;
import cn.ac.iie.service.update.relationship.LocateSubscriber2Ip;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

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

    private static BaseClickhouseData baseClickhouseData = new BaseClickhouseData();
    private CountDownLatch countDownLatch;

    public void updateArango(){
        long start = System.currentTimeMillis();
        try {
            BaseArangoData baseArangoData = new BaseArangoData();

            baseArangoData.readHistoryData("FQDN", historyVertexFqdnMap,BaseDocument.class);
            updateVertexFqdn();

            baseArangoData.readHistoryData("IP", historyVertexIpMap,BaseDocument.class);
            updateVertexIp();

            baseArangoData.readHistoryData("SUBSCRIBER", historyVertexSubscriberMap,BaseDocument.class);
            updateVertexSubscriber();

            baseArangoData.readHistoryData("R_LOCATE_FQDN2IP", historyRelationFqdnAddressIpMap,BaseEdgeDocument.class);
            updateRelationFqdnAddressIp();

            baseArangoData.readHistoryData("R_VISIT_IP2FQDN", historyRelationIpVisitFqdnMap,BaseEdgeDocument.class);
            updateRelationIpVisitFqdn();

            baseArangoData.readHistoryData("R_LOCATE_SUBSCRIBER2IP", historyRelationSubsciberLocateIpMap,BaseEdgeDocument.class);
            updateRelationshipSubsciberLocateIp();

            long last = System.currentTimeMillis();
            LOG.info("更新图数据库时间共计："+(last - start));
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            arangoManger.clean();
            pool.shutdown();
        }
    }

    private void updateVertexFqdn(){
        try {
            long start = System.currentTimeMillis();
            baseClickhouseData.baseVertexFqdn();
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, ArrayList<BaseDocument>> tmpMap = newVertexFqdnMap.get(i);
                Document updateFqdn = new Fqdn(tmpMap, arangoManger, "FQDN", historyVertexFqdnMap,countDownLatch);
                pool.executor(updateFqdn);
            }
            countDownLatch.await();
            long last = System.currentTimeMillis();
            LOG.info("FQDN vertex 更新完毕,共耗时："+(last-start));
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            historyVertexFqdnMap.clear();
            newVertexFqdnMap.clear();
        }
    }

    private void updateVertexSubscriber(){
        try {
            long start = System.currentTimeMillis();
            baseClickhouseData.baseVertexSubscriber();
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, ArrayList<BaseDocument>> tmpMap = newVertexSubscriberMap.get(i);
                Subscriber updateSubscriber = new Subscriber(tmpMap, arangoManger, "SUBSCRIBER", historyVertexSubscriberMap,countDownLatch);
                pool.executor(updateSubscriber);
            }
            countDownLatch.await();
            long last = System.currentTimeMillis();
            LOG.info("SUBSCRIBER vertex 更新完毕,共耗时："+(last-start));
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            historyVertexSubscriberMap.clear();
            newVertexSubscriberMap.clear();
        }
    }

    private void updateRelationshipSubsciberLocateIp(){
        try {
            long start = System.currentTimeMillis();
            baseClickhouseData.baseRelationshipSubscriberLocateIp();
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, ArrayList<BaseEdgeDocument>> tmpMap = newRelationSubsciberLocateIpMap.get(i);
                LocateSubscriber2Ip locateSubscriber2Ip = new LocateSubscriber2Ip(tmpMap, arangoManger, "R_LOCATE_SUBSCRIBER2IP", historyRelationSubsciberLocateIpMap, countDownLatch);
                pool.executor(locateSubscriber2Ip);
            }
            countDownLatch.await();
            long last = System.currentTimeMillis();
            LOG.info("R_LOCATE_SUBSCRIBER2IP relationship 更新完毕,共耗时："+(last-start));
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            historyRelationSubsciberLocateIpMap.clear();
            newRelationSubsciberLocateIpMap.clear();
        }
    }

    private void updateVertexIp(){
        try {
            long start = System.currentTimeMillis();
            baseClickhouseData.baseVertexIp();
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, ArrayList<BaseDocument>> tmpMap = newVertexIpMap.get(i);
                Ip updateIp = new Ip(tmpMap, arangoManger, "IP", historyVertexIpMap, countDownLatch);
                pool.executor(updateIp);
            }
            countDownLatch.await();
            long last = System.currentTimeMillis();
            LOG.info("IP vertex 更新完毕，共耗时："+(last-start));
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            historyVertexIpMap.clear();
            newVertexIpMap.clear();
        }
    }

    private void updateRelationFqdnAddressIp(){
        try {
            long start = System.currentTimeMillis();
            baseClickhouseData.baseRelationshipFqdnAddressIp();
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, ArrayList<BaseEdgeDocument>> tmpMap = newRelationFqdnAddressIpMap.get(i);
                LocateFqdn2Ip fqdnAddressIp = new LocateFqdn2Ip(tmpMap, arangoManger, "R_LOCATE_FQDN2IP", historyRelationFqdnAddressIpMap, countDownLatch);
                pool.executor(fqdnAddressIp);
            }
            countDownLatch.await();
            long last = System.currentTimeMillis();
            LOG.info("R_LOCATE_FQDN2IP relationship 更新完毕，共耗时："+(last-start));
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            historyRelationFqdnAddressIpMap.clear();
            newRelationFqdnAddressIpMap.clear();
        }
    }

    private void updateRelationIpVisitFqdn(){
        try {
            long start = System.currentTimeMillis();
            baseClickhouseData.baseRelationshipIpVisitFqdn();
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, ArrayList<BaseEdgeDocument>> tmpMap = newRelationIpVisitFqdnMap.get(i);
                VisitIp2Fqdn ipVisitFqdn = new VisitIp2Fqdn(tmpMap,arangoManger,"R_VISIT_IP2FQDN", historyRelationIpVisitFqdnMap,countDownLatch);
                pool.executor(ipVisitFqdn);
            }
            countDownLatch.await();
            long last = System.currentTimeMillis();
            LOG.info("R_VISIT_IP2FQDN ralationship 更新完毕，共耗时："+(last-start));
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            historyRelationIpVisitFqdnMap.clear();
            newRelationIpVisitFqdnMap.clear();
        }
    }

}
