package cn.ac.iie.service.vertex;

import cn.ac.iie.service.update.Vertex;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class Subscriber extends Vertex {

    public Subscriber(HashMap<String, ArrayList<BaseDocument>> newDocumentHashMap,
                      ArangoDBConnect arangoManger,
                      String collectionName,
                      ConcurrentHashMap<String, BaseDocument> historyDocumentMap,
                      CountDownLatch countDownLatch) {
        super(newDocumentHashMap, arangoManger, collectionName, historyDocumentMap, countDownLatch);
    }
}
