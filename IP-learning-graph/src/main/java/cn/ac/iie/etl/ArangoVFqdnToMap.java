package cn.ac.iie.etl;

import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ArangoVFqdnToMap implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ArangoVFqdnToMap.class);

    private ArangoDBConnect arangoDBConnect;
    private long finalMinTime;
    private long diffTime;
    private int threadNumber;

    private ArangoVFqdnToMap(){}

    public ArangoVFqdnToMap(ArangoDBConnect arangoDBConnect, long finalMinTime, long diffTime, int threadNumber) {
        this.arangoDBConnect = arangoDBConnect;
        this.finalMinTime = finalMinTime;
        this.diffTime = diffTime;
        this.threadNumber = threadNumber;
    }

    public void run() {
        String name = Thread.currentThread().getName();
        long maxThreadTime = finalMinTime + (threadNumber + 1)* diffTime;
        long minThreadTime = finalMinTime + threadNumber * diffTime;
        String query = "FOR doc IN FQDN filter doc.FIRST_FOUND_TIME >= "+minThreadTime+" and doc.FIRST_FOUND_TIME <= "+maxThreadTime+" RETURN doc";
        LOG.info(name+":"+query);
        long s = System.currentTimeMillis();
        ArangoCursor<BaseDocument> docs = arangoDBConnect.executorQuery(query, BaseDocument.class);

        try {
            if (docs != null){
                List<BaseDocument> baseDocuments = docs.asListRemaining();
                int i = 0;
                for (BaseDocument doc:baseDocuments){
                    String key = doc.getKey();
                    BaseArangoData.v_Fqdn_Map.put(key,doc);
                    i++;
                }
                LOG.info(name+":共处理数据"+ i);
                long l = System.currentTimeMillis();
                LOG.info(name+"运行时间："+(l-s));
            }else {
                LOG.warn("获取VFqdn异常，结果为空");
            }
        }catch (Exception e){
            LOG.error(e.getMessage());
        }
    }
}
