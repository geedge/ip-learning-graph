package cn.ac.iie.etl;

import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.List;

public class ArangoEFqdnAddressIpToMap implements Runnable{

    private ArangoDBConnect arangoDBConnect;
    private long finalMinTime;
    private long diffTime;
    private int threadNumber;

    private ArangoEFqdnAddressIpToMap(){}

    public ArangoEFqdnAddressIpToMap(ArangoDBConnect arangoDBConnect, long finalMinTime, long diffTime, int threadNumber) {
        this.arangoDBConnect = arangoDBConnect;
        this.finalMinTime = finalMinTime;
        this.diffTime = diffTime;
        this.threadNumber = threadNumber;
    }

    public void run() {
        String name = Thread.currentThread().getName();
        long maxThreadTime = finalMinTime + (threadNumber + 1) * diffTime;
        long minThreadTime = finalMinTime + threadNumber * diffTime;
        String query = "FOR doc IN E_ADDRESS_V_FQDN_TO_V_IP filter doc.FIRST_FOUND_TIME >= " + minThreadTime + " and doc.FIRST_FOUND_TIME <= " + maxThreadTime + " RETURN doc";
        System.out.println(name + ":" + query);
        long s = System.currentTimeMillis();
        ArangoCursor<BaseEdgeDocument> docs = arangoDBConnect.executorQuery(query, BaseEdgeDocument.class);
        if (docs != null){
            List<BaseEdgeDocument> baseDocuments = docs.asListRemaining();
            int i = 0;
            for (BaseEdgeDocument doc : baseDocuments) {
                String key = doc.getKey();
                BaseArangoData.e_Fqdn_Address_Ip_Map.put(key, doc);
                i++;
            }
            System.out.println(name + ":共处理数据" + i);
            long l = System.currentTimeMillis();
            System.out.println(name + "运行时间：" + (l - s));
        }else {
            System.out.println("查询异常");
        }
    }
}
