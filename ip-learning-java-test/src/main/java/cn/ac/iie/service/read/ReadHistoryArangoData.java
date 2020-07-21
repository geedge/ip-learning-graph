package cn.ac.iie.service.read;

import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * @author wlh
 * 多线程全量读取arangoDb历史数据，封装到map
 */
public class ReadHistoryArangoData<T extends BaseDocument> extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ReadHistoryArangoData.class);

    private ArangoDBConnect arangoConnect;
    private String query;
    private ConcurrentHashMap<String, T> map;
    private Class<T> type;
    private String table;
    private CountDownLatch countDownLatch;

    public ReadHistoryArangoData(ArangoDBConnect arangoConnect, String query, ConcurrentHashMap<String, T> map, Class<T> type, String table,CountDownLatch countDownLatch) {
        this.arangoConnect = arangoConnect;
        this.query = query;
        this.map = map;
        this.type = type;
        this.table = table;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        try {
            long s = System.currentTimeMillis();
            ArangoCursor<T> docs = arangoConnect.executorQuery(query, type);
            if (docs != null) {
                List<T> baseDocuments = docs.asListRemaining();
                int i = 0;
                for (T doc : baseDocuments) {
                    String key = doc.getKey();
                    map.put(key, doc);
                    i++;
                }
                long l = System.currentTimeMillis();
                LOG.info(query + "\n读取数据" + i + "条，运行时间：" + (l - s));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            countDownLatch.countDown();
        }
    }

    private void updateProtocolDocument(T doc) {
        if (doc.getProperties().containsKey("PROTOCOL_TYPE")) {
            for (String protocol : ReadClickhouseData.PROTOCOL_SET) {
                String protocolRecent = protocol + "_CNT_RECENT";
                ArrayList<Long> cntRecent = (ArrayList<Long>) doc.getAttribute(protocolRecent);
                Long[] cntRecentsSrc = cntRecent.toArray(new Long[cntRecent.size()]);
                Long[] cntRecentsDst = new Long[24];
                System.arraycopy(cntRecentsSrc, 0, cntRecentsDst, 1, cntRecentsSrc.length - 1);
                cntRecentsDst[0] = 0L;
                doc.addAttribute(protocolRecent, cntRecentsDst);
            }
        }
    }

}
