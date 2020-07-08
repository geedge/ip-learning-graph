package cn.ac.iie.etl.read;

import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wlh
 * 多线程全量读取arangoDb历史数据，封装到map
 */
public class ReadHistoryArangoData extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ReadHistoryArangoData.class);

    private ArangoDBConnect arangoDBConnect;
    private String query;
    private ConcurrentHashMap<String, BaseEdgeDocument> map;

    public ReadHistoryArangoData(ArangoDBConnect arangoDBConnect, String query, ConcurrentHashMap<String, BaseEdgeDocument> map) {
        this.arangoDBConnect = arangoDBConnect;
        this.query = query;
        this.map = map;
    }

    @Override
    public void run() {
        long s = System.currentTimeMillis();
        ArangoCursor<BaseEdgeDocument> docs = arangoDBConnect.executorQuery(query, BaseEdgeDocument.class);
        if (docs != null){
            List<BaseEdgeDocument> baseDocuments = docs.asListRemaining();
            int i = 0;
            for (BaseEdgeDocument doc : baseDocuments) {
                String key = doc.getKey();
                map.put(key, doc);
                i++;
            }
            long l = System.currentTimeMillis();
            LOG.info(query+ "\n处理IP数据" + i + "条，运行时间：" + (l - s));
        }
    }
}
