package cn.ac.iie.test;

import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.dao.BaseClickhouseData;
import cn.ac.iie.utils.ArangoDBConnect;
import cn.ac.iie.utils.ExecutorThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class IpLearningApplicationTest {
    private static final Logger LOG = LoggerFactory.getLogger(IpLearningApplicationTest.class);

    public static void main(String[] args) {

        long startA = System.currentTimeMillis();
        BaseArangoData.BaseVFqdnDataMap();
        BaseArangoData.BaseVIpDataMap();
        BaseArangoData.BaseEFqdnAddressIpDataMap();
        BaseArangoData.BaseEIpVisitFqdnDataMap();


        ExecutorThreadPool.shutdown();
        ExecutorThreadPool.awaitThreadTask();
        long lastA = System.currentTimeMillis();
        LOG.info("读取ArangoDb时间："+(lastA - startA));

        long startC = System.currentTimeMillis();

        try {
            CountDownLatch countDownLatch = new CountDownLatch(4);

            new Thread(() -> {
                BaseClickhouseData.BaseVFqdn();
                countDownLatch.countDown();
            }).start();

            new Thread(() -> {
                BaseClickhouseData.BaseVIp();
                countDownLatch.countDown();
            }).start();

            new Thread(() -> {
                BaseClickhouseData.BaseEFqdnAddressIp();
                countDownLatch.countDown();
            }).start();

            new Thread(() -> {
                BaseClickhouseData.BaseEIpVisitFqdn();
                countDownLatch.countDown();
            }).start();

            try {
                countDownLatch.await();
                LOG.info("主线程等待完毕");
            }catch (Exception e){
                LOG.error("主线程阻塞异常：\n"+e.toString());
            }

            //        BaseClickhouseData.BaseEFqdnAddressIp();
            long lastC = System.currentTimeMillis();
            LOG.info("更新ArangoDb时间："+(lastC - startC));
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ArangoDBConnect.clean();
        }

        LOG.info("v_Fqdn_Map大小："+BaseArangoData.v_Fqdn_Map.size());
        LOG.info("v_Ip_Map大小："+BaseArangoData.v_Ip_Map.size());
        LOG.info("e_Fqdn_Address_Ip_Map大小："+BaseArangoData.e_Fqdn_Address_Ip_Map.size());
        LOG.info("e_Ip_Visit_Fqdn_Map大小："+BaseArangoData.e_Ip_Visit_Fqdn_Map.size());

    }
}
