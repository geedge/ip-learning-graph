package cn.ac.iie.service.relationship;

import cn.ac.iie.service.update.Relationship;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class LocateSubscriber2Ip extends Relationship {

    public LocateSubscriber2Ip(HashMap<String, HashMap<String, BaseEdgeDocument>> newDocumentHashMap,
                               ArangoDBConnect arangoManger,
                               String collectionName,
                               ConcurrentHashMap<String, BaseEdgeDocument> historyDocumentMap,
                               CountDownLatch countDownLatch) {
        super(newDocumentHashMap, arangoManger, collectionName, historyDocumentMap, countDownLatch);
    }

    @Override
    protected BaseEdgeDocument mergeRelationship(HashMap<String, BaseEdgeDocument> newEdgeDocumentSchemaMap) {
        BaseEdgeDocument doc = null;
        if (newEdgeDocumentSchemaMap.size() == 1){
            Set<String> strings = newEdgeDocumentSchemaMap.keySet();
            for (String key:strings){
                doc = newEdgeDocumentSchemaMap.getOrDefault(key,null);
            }
        }
        return doc;
    }

    @Override
    protected void updateFunction(BaseEdgeDocument newEdgeDocument, BaseEdgeDocument historyEdgeDocument) {
        super.updateFoundTime(newEdgeDocument,historyEdgeDocument);
    }
}
