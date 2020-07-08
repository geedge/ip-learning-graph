package cn.ac.iie.dao;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.etl.relationship.LocateFqdn2Ip;
import cn.ac.iie.etl.relationship.VisitIp2Fqdn;
import cn.ac.iie.etl.vertex.Fqdn;
import cn.ac.iie.etl.vertex.Ip;
import cn.ac.iie.utils.ArangoDBConnect;
import cn.ac.iie.utils.ExecutorThreadPool;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class UpdateGraphData {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateGraphData.class);
    private static ExecutorThreadPool pool = ExecutorThreadPool.getInstance();
    private static ArangoDBConnect arangoManger = ArangoDBConnect.getInstance();

    private CountDownLatch countDownLatch;

    public void updateArango(){
        long startC = System.currentTimeMillis();
        try {
            BaseClickhouseData baseClickhouseData = new BaseClickhouseData();
            baseClickhouseData.BaseVFqdn();
            updateVertexFqdn();

            baseClickhouseData.BaseVIp();
            updateVertexIp();

            baseClickhouseData.BaseEFqdnAddressIp();
            updateRelationFqdnAddressIp();

            baseClickhouseData.BaseEIpVisitFqdn();
            updateRelationIpVisitFqdn();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ArangoDBConnect.clean();
        }
        long lastC = System.currentTimeMillis();
        LOG.info("更新ArangoDb时间："+(lastC - startC));
    }

    private void updateVertexFqdn(){
        try {
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, ArrayList<BaseDocument>> stringArrayListHashMap = BaseClickhouseData.vFqdnMap.get(i);
                LOG.info("vFqdn baseDocumentHashMap大小：" + stringArrayListHashMap.size());
                Fqdn updateVFqdn = new Fqdn(stringArrayListHashMap, arangoManger, "FQDN", BaseArangoData.v_Fqdn_Map,countDownLatch);
                updateVFqdn.run();
            }
            countDownLatch.await();
            LOG.info("---------FQDN vertex 更新完毕---------");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void updateVertexIp(){
        try {
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, ArrayList<BaseDocument>> stringArrayListHashMap = BaseClickhouseData.vIpMap.get(i);
                LOG.info("vIp baseDocumentHashMap大小：" + stringArrayListHashMap.size());
                Ip updateVIp = new Ip(stringArrayListHashMap, arangoManger, "IP", BaseArangoData.v_Ip_Map, countDownLatch);
                updateVIp.run();
            }
            countDownLatch.await();
            LOG.info("----------IP vertex 更新完毕-------------");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void updateRelationFqdnAddressIp(){
        try {
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, HashMap<String, BaseEdgeDocument>> baseDocumentHashMap = BaseClickhouseData.eFqdnAddressIpMap.get(i);
                LOG.info("EFqdnAddressIp baseDocumentHashMap大小：" + baseDocumentHashMap.size());
                LocateFqdn2Ip updateEFqdnAddressIp = new LocateFqdn2Ip(baseDocumentHashMap, arangoManger, "R_LOCATE_FQDN2IP", BaseArangoData.e_Fqdn_Address_Ip_Map, countDownLatch);
                updateEFqdnAddressIp.run();
            }
            countDownLatch.await();
            LOG.info("------------R_LOCATE_FQDN2IP relationship 更新完毕----------------");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void updateRelationIpVisitFqdn(){
        try {
            countDownLatch = new CountDownLatch(ApplicationConfig.THREAD_POOL_NUMBER);
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, HashMap<String, BaseEdgeDocument>> baseDocumentHashMap = BaseClickhouseData.eIpVisitFqdnMap.get(i);
                LOG.info("EIpVisitFqdn baseDocumentHashMap大小：" + baseDocumentHashMap.size());
                VisitIp2Fqdn updateEIpVisitFqdn = new VisitIp2Fqdn(baseDocumentHashMap,arangoManger,"R_VISIT_IP2FQDN",BaseArangoData.e_Ip_Visit_Fqdn_Map,countDownLatch);
                updateEIpVisitFqdn.run();
            }
            countDownLatch.await();
            LOG.info("---------------R_VISIT_IP2FQDN ralationship 更新完毕----------------");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
