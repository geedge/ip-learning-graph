package cn.ac.iie;

import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.utils.ArangoDBConnect;
import cn.ac.iie.utils.ExecutorThreadPool;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import org.junit.After;
import org.junit.Test;

import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

public class TestReadArango {
    private static ExecutorThreadPool pool = ExecutorThreadPool.getInstance();
    private static ArangoDBConnect arangoManger = ArangoDBConnect.getInstance();

    private static BaseArangoData baseArangoData = new BaseArangoData();


    @Test
    public void testReadFqdnFromArango() {
        ConcurrentHashMap<Integer, ConcurrentHashMap<String, BaseDocument>> historyData =
                baseArangoData.readHistoryData("FQDN", BaseDocument.class);
        printMap(historyData);
    }

    @Test
    public void testReadFqdnLocIpFromArango() {
        ConcurrentHashMap<Integer, ConcurrentHashMap<String, BaseEdgeDocument>> ip =
                baseArangoData.readHistoryData("R_LOCATE_FQDN2IP", BaseEdgeDocument.class);
        printMap(ip);
    }

    private <T extends BaseDocument> void printMap(ConcurrentHashMap<Integer, ConcurrentHashMap<String, T>> historyData) {
        ConcurrentHashMap<String, T> map = historyData.get(2);
        Enumeration<String> keys = map.keys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            T document = map.get(key);
            System.out.println(document.toString());
        }
    }


    @After
    public void clearSource() {
        pool.shutdown();
        arangoManger.clean();
    }


}
