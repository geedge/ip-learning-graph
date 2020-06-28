package cn.ac.iie.etl;

import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;

import java.util.List;

public class ArangoVFqdnToMap implements Runnable {

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
        String query = "FOR doc IN V_FQDN filter doc.FIRST_FOUND_TIME >= "+minThreadTime+" and doc.FIRST_FOUND_TIME <= "+maxThreadTime+" RETURN doc";
        System.out.println(name+":"+query);
        long s = System.currentTimeMillis();
        ArangoCursor<BaseDocument> docs = arangoDBConnect.executorQuery(query, BaseDocument.class);

        if (docs != null){
            List<BaseDocument> baseDocuments = docs.asListRemaining();
            int i = 0;
            for (BaseDocument doc:baseDocuments){
                String key = doc.getKey();
                BaseArangoData.v_Fqdn_Map.put(key,doc);
                i++;
            }
            System.out.println(name+":共处理数据"+ i);
            long l = System.currentTimeMillis();
            System.out.println(name+"运行时间："+(l-s));
        }else {
            System.out.println("查询异常");
        }
    }
}
