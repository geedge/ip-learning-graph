package cn.ac.iie.test;

import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.dao.BaseClickhouseData;
import cn.ac.iie.utils.ArangoDBConnect;
import cn.ac.iie.utils.ExecutorThreadPool;

import java.util.concurrent.CountDownLatch;

public class IpLearningApplicationTest {

    public static void main(String[] args) {
        long startA = System.currentTimeMillis();
        BaseArangoData baseArangoData = new BaseArangoData();
        baseArangoData.baseDocumentDataMap();


        long lastA = System.currentTimeMillis();
        System.out.println("读取ArangoDb时间："+(lastA - startA));


        /*

        long startC = System.currentTimeMillis();
        CountDownLatch countDownLatch = new CountDownLatch(4);
        new Thread(new Runnable() {
            @Override
            public void run() {
                BaseClickhouseData.BaseVFqdn();
                countDownLatch.countDown();
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                BaseClickhouseData.BaseVIp();
                countDownLatch.countDown();
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                BaseClickhouseData.BaseEFqdnAddressIp();
                countDownLatch.countDown();
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                BaseClickhouseData.BaseEIpVisitFqdn();
                countDownLatch.countDown();
            }
        }).start();
        try {
            countDownLatch.await();
        }catch (Exception e){
            e.printStackTrace();
        }
        long lastC = System.currentTimeMillis();
        System.out.println("更新ArangoDb时间："+(lastC - startC));

        */

        System.out.println(BaseArangoData.v_Fqdn_Map.size());
        System.out.println(BaseArangoData.v_Ip_Map.size());
        System.out.println(BaseArangoData.e_Fqdn_Address_Ip_Map.size());
        System.out.println(BaseArangoData.e_Ip_Visit_Fqdn_Map.size());

        ArangoDBConnect.clean();
    }
}
