package cn.ac.iie.service.update.relationship;

import cn.ac.iie.service.update.Relationship;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class LocateSubscriber2Ip extends Relationship {

    public LocateSubscriber2Ip(HashMap<String, ArrayList<BaseEdgeDocument>> newDocumentHashMap,
                               ArangoDBConnect arangoManger,
                               String collectionName,
                               ConcurrentHashMap<String, BaseEdgeDocument> historyDocumentMap,
                               CountDownLatch countDownLatch) {
        super(newDocumentHashMap, arangoManger, collectionName, historyDocumentMap, countDownLatch);
    }
}
