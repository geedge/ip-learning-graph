package cn.ac.iie.service.read;

import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wlh
 * 多线程全量读取arangoDb历史数据，封装到map
 */
public class ReadHistoryArangoData<T extends BaseDocument> extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ReadHistoryArangoData.class);

    private ArangoDBConnect arangoDBConnect;
    private String query;
    private ConcurrentHashMap<String, T> map;
    private Class<T> type;

    public ReadHistoryArangoData(ArangoDBConnect arangoDBConnect, String query, ConcurrentHashMap<String, T> map,Class<T> type) {
        this.arangoDBConnect = arangoDBConnect;
        this.query = query;
        this.map = map;
        this.type = type;
    }

    @Override
    public void run() {
        long s = System.currentTimeMillis();
        ArangoCursor<T> docs = arangoDBConnect.executorQuery(query, type);
        if (docs != null){
            List<T> baseDocuments = docs.asListRemaining();
            int i = 0;
            for (T doc : baseDocuments) {
                String key = doc.getKey();
                map.put(key, doc);
                i++;
            }
            long l = System.currentTimeMillis();
            LOG.info(query+ "\n处理数据" + i + "条，运行时间：" + (l - s));
        }
    }
}
