package cn.ac.iie.service.update;

import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseDocument;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * @author wlh
 * 多线程更新vertex数据
 */
public class Vertex extends Document<BaseDocument> {

    public Vertex(HashMap<String, ArrayList<BaseDocument>> newDocumentHashMap,
                  ArangoDBConnect arangoManger,
                  String collectionName,
                  ConcurrentHashMap<String, BaseDocument> historyDocumentMap,
                  CountDownLatch countDownLatch) {
        super(newDocumentHashMap, arangoManger, collectionName, historyDocumentMap, countDownLatch,BaseDocument.class);
    }

    @Override
    protected void updateFunction(BaseDocument newDocument, BaseDocument historyDocument) {
        super.updateFunction(newDocument, historyDocument);
    }

    @Override
    protected void mergeFunction(Map<String, Object> properties, BaseDocument doc) {
        super.mergeFunction(properties, doc);
    }

    @Override
    public void run() {
        super.run();
    }

}
