package cn.ac.iie.service.ingestion;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static cn.ac.iie.service.ingestion.ReadClickhouseData.*;

/**
 * @author wlh
 * 多线程全量读取arangoDb历史数据，封装到map
 */
@SuppressWarnings("unchecked")
public class ReadHistoryArangoData<T extends BaseDocument> extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ReadHistoryArangoData.class);

    private ArangoDBConnect arangoConnect;
    private String query;
    private ConcurrentHashMap<Integer, ConcurrentHashMap<String, T>> map;
    private Class<T> type;
    private String table;
    private CountDownLatch countDownLatch;

    public ReadHistoryArangoData(ArangoDBConnect arangoConnect,
                                 String query,
                                 ConcurrentHashMap<Integer, ConcurrentHashMap<String, T>> map,
                                 Class<T> type,
                                 String table,
                                 CountDownLatch countDownLatch) {
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
                    switch (table) {
                        case "R_LOCATE_FQDN2IP":
                            updateProtocolDocument(doc);
                            deleteDistinctClientIpByTime(doc);
                            break;
                        case "R_VISIT_IP2FQDN":
                            updateProtocolDocument(doc);
                            break;
                        default:
                    }
                    int hashCode = Math.abs(key.hashCode()) % ApplicationConfig.THREAD_POOL_NUMBER;
                    ConcurrentHashMap<String, T> tmpMap = map.get(hashCode);
                    tmpMap.put(key, doc);
                    i++;
                }
                long l = System.currentTimeMillis();
                LOG.info(query + "\n读取" + i + "条数据，运行时间：" + (l - s));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            countDownLatch.countDown();
            LOG.info("本线程读取完毕，剩余线程数量：" + countDownLatch.getCount());
        }
    }

    private void updateProtocolDocument(T doc) {
        if (doc.getProperties().containsKey("PROTOCOL_TYPE")) {
            for (String protocol : ReadClickhouseData.PROTOCOL_SET) {
                String protocolRecent = protocol + "_CNT_RECENT";
                ArrayList<Long> cntRecent = (ArrayList<Long>) doc.getAttribute(protocolRecent);
                Long[] cntRecentsSrc = cntRecent.toArray(new Long[0]);
                Long[] cntRecentsDst = new Long[RECENT_COUNT_HOUR];
                System.arraycopy(cntRecentsSrc, 0, cntRecentsDst, 1, cntRecentsSrc.length - 1);
                cntRecentsDst[0] = 0L;
                doc.addAttribute(protocolRecent, cntRecentsDst);
            }
        }
    }

    private void deleteDistinctClientIpByTime(T doc) {
        ArrayList<String> distCip = (ArrayList<String>) doc.getAttribute("DIST_CIP");
        ArrayList<Long> distCipTs = (ArrayList<Long>) doc.getAttribute("DIST_CIP_TS");
        distCipTs.add(currentHour - RECENT_COUNT_HOUR * 3600);
        Collections.sort(distCipTs);
        int index = distCipTs.indexOf(currentHour - RECENT_COUNT_HOUR * 3600);
        String[] distCipArr = new String[index];
        long[] disCipTsArr = new long[index];
        if (distCip.size() + 1 == distCipTs.size()){
            for (int i = 0; i < index; i++) {
                distCipArr[i] = distCip.get(i);
                disCipTsArr[i] = distCipTs.get(i);
            }
        }
        doc.updateAttribute("DIST_CIP", distCipArr);
        doc.updateAttribute("DIST_CIP_TS", disCipTsArr);
    }

}
