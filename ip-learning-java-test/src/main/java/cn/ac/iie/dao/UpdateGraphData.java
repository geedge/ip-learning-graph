package cn.ac.iie.dao;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.service.relationship.LocateFqdn2Ip;
import cn.ac.iie.service.relationship.LocateSubscriber2Ip;
import cn.ac.iie.service.relationship.VisitIp2Fqdn;
import cn.ac.iie.service.vertex.Fqdn;
import cn.ac.iie.service.vertex.Ip;
import cn.ac.iie.service.vertex.Subscriber;
import cn.ac.iie.utils.ArangoDBConnect;
import cn.ac.iie.utils.ExecutorThreadPool;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

/**
 * 更新图数据库业务类
 */
public class UpdateGraphData {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateGraphData.class);
    private static ExecutorThreadPool pool = ExecutorThreadPool.getInstance();
    private static ArangoDBConnect arangoManger = ArangoDBConnect.getInstance();

    private static BaseClickhouseData baseClickhouseData = new BaseClickhouseData();
    private CountDownLatch countDownLatch;

    public void updateArango(){
        long startC = System.currentTimeMillis();
        try {
            updateVertexFqdn();
            updateVertexIp();
            updateRelationFqdnAddressIp();
            updateRelationIpVisitFqdn();
            updateVertexSubscriber();
            updateRelationshipSubsciberLocateIp();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            arangoManger.clean();
        }
        long lastC = System.currentTimeMillis();
        LOG.info("更新ArangoDb时间："+(lastC - startC));
    }

    private void updateVertexFqdn(){
        baseClickhouseData.baseVertexFqdn();
        try {
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, ArrayList<BaseDocument>> stringArrayListHashMap = BaseClickhouseData.vFqdnMap.get(i);
                LOG.info("vFqdn baseDocumentHashMap大小：" + stringArrayListHashMap.size());
                Fqdn updateFqdn = new Fqdn(stringArrayListHashMap, arangoManger, "FQDN", BaseArangoData.v_Fqdn_Map,countDownLatch);
                updateFqdn.run();
            }
            countDownLatch.await();
            LOG.info("---------FQDN vertex 更新完毕---------");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void updateVertexSubscriber(){
        baseClickhouseData.baseVertexSubscriber();
        try {
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, ArrayList<BaseDocument>> stringArrayListHashMap = BaseClickhouseData.vSubscriberMap.get(i);
                LOG.info("vSubscriber baseDocumentHashMap大小：" + stringArrayListHashMap.size());
                Subscriber updateSubscriber = new Subscriber(stringArrayListHashMap, arangoManger, "SUBSCRIBER", BaseArangoData.v_Subscriber_Map,countDownLatch);
                updateSubscriber.run();
            }
            countDownLatch.await();
            LOG.info("---------SUBSCRIBER vertex 更新完毕---------");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void updateRelationshipSubsciberLocateIp(){
        baseClickhouseData.baseRelationshipSubscriberLocateIp();
        try {
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, ArrayList<BaseEdgeDocument>> baseDocumentHashMap = BaseClickhouseData.eSubsciberLocateIpMap.get(i);
                LOG.info("ESubsciberLocateIp baseDocumentHashMap大小：" + baseDocumentHashMap.size());
                LocateSubscriber2Ip locateSubscriber2Ip = new LocateSubscriber2Ip(baseDocumentHashMap, arangoManger, "R_LOCATE_SUBSCRIBER2IP", BaseArangoData.e_Subsciber_Locate_Ip_Map, countDownLatch);
                locateSubscriber2Ip.run();
            }
            countDownLatch.await();
            LOG.info("------------R_LOCATE_SUBSCRIBER2IP relationship 更新完毕----------------");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void updateVertexIp(){
        baseClickhouseData.baseVertexIp();
        try {
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, ArrayList<BaseDocument>> stringArrayListHashMap = BaseClickhouseData.vIpMap.get(i);
                LOG.info("vIp baseDocumentHashMap大小：" + stringArrayListHashMap.size());
                Ip updateIp = new Ip(stringArrayListHashMap, arangoManger, "IP", BaseArangoData.v_Ip_Map, countDownLatch);
                updateIp.run();
            }
            countDownLatch.await();
            LOG.info("----------IP vertex 更新完毕-------------");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void updateRelationFqdnAddressIp(){
        baseClickhouseData.baseRelationshipFqdnAddressIp();
        try {
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, ArrayList<BaseEdgeDocument>> baseDocumentHashMap = BaseClickhouseData.eFqdnAddressIpMap.get(i);
                LOG.info("EFqdnAddressIp baseDocumentHashMap大小：" + baseDocumentHashMap.size());
                LocateFqdn2Ip fqdnAddressIp = new LocateFqdn2Ip(baseDocumentHashMap, arangoManger, "R_LOCATE_FQDN2IP", BaseArangoData.e_Fqdn_Address_Ip_Map, countDownLatch);
                fqdnAddressIp.run();
            }
            countDownLatch.await();
            LOG.info("------------R_LOCATE_FQDN2IP relationship 更新完毕----------------");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void updateRelationIpVisitFqdn(){
        baseClickhouseData.baseRelationshipIpVisitFqdn();
        try {
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, ArrayList<BaseEdgeDocument>> baseDocumentHashMap = BaseClickhouseData.eIpVisitFqdnMap.get(i);
                LOG.info("EIpVisitFqdn baseDocumentHashMap大小：" + baseDocumentHashMap.size());
                VisitIp2Fqdn ipVisitFqdn = new VisitIp2Fqdn(baseDocumentHashMap,arangoManger,"R_VISIT_IP2FQDN",BaseArangoData.e_Ip_Visit_Fqdn_Map,countDownLatch);
                ipVisitFqdn.run();
            }
            countDownLatch.await();
            LOG.info("---------------R_VISIT_IP2FQDN ralationship 更新完毕----------------");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
