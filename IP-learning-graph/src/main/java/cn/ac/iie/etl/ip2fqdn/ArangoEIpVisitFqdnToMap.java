package cn.ac.iie.etl.ip2fqdn;

import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ArangoEIpVisitFqdnToMap implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ArangoEIpVisitFqdnToMap.class);
    private ArangoDBConnect arangoDBConnect;
    private long finalMinTime;
    private long diffTime;
    private int threadNumber;

    public ArangoEIpVisitFqdnToMap(ArangoDBConnect arangoDBConnect, long finalMinTime, long diffTime, int threadNumber) {
        this.arangoDBConnect = arangoDBConnect;
        this.finalMinTime = finalMinTime;
        this.diffTime = diffTime;
        this.threadNumber = threadNumber;
    }

    public void run() {
        String name = Thread.currentThread().getName();
        long maxThreadTime = finalMinTime + (threadNumber + 1) * diffTime;
        long minThreadTime = finalMinTime + threadNumber * diffTime;
        String query = "FOR doc IN R_VISIT_IP2FQDN filter doc.FIRST_FOUND_TIME >= " + minThreadTime + " and doc.FIRST_FOUND_TIME <= " + maxThreadTime + " RETURN doc";
//        LOG.info(name + ":" + query);
        long s = System.currentTimeMillis();
        ArangoCursor<BaseEdgeDocument> docs = arangoDBConnect.executorQuery(query, BaseEdgeDocument.class);

        if (docs != null){
            List<BaseEdgeDocument> baseDocuments = docs.asListRemaining();
            int i = 0;
            for (BaseEdgeDocument doc : baseDocuments) {
                String key = doc.getKey();
                BaseArangoData.e_Ip_Visit_Fqdn_Map.put(key, doc);
                i++;
            }
            long l = System.currentTimeMillis();
            LOG.info(query+ "\n处理R_VISIT_IP2FQDN数据" + i + "条，运行时间：" + (l - s));
        }else {
            LOG.warn("查询R_VISIT_IP2FQDN异常，结果为空");
        }
    }
}
