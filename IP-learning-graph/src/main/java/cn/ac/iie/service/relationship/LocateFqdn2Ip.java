package cn.ac.iie.service.relationship;

import cn.ac.iie.service.update.Relationship;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class LocateFqdn2Ip extends Relationship {

    public LocateFqdn2Ip(HashMap<String, HashMap<String, BaseEdgeDocument>> newDocumentHashMap,
                         ArangoDBConnect arangoManger,
                         String collectionName,
                         ConcurrentHashMap<String, BaseEdgeDocument> historyDocumentMap,
                         CountDownLatch countDownLatch) {
        super(newDocumentHashMap, arangoManger, collectionName, historyDocumentMap,countDownLatch);
    }

    @Override
    protected void mergeFunction(Map<String, Object> properties, BaseEdgeDocument schemaEdgeDoc){
        super.mergeFunction(properties,schemaEdgeDoc);
        super.mergeDistinctClientIp(properties,schemaEdgeDoc);
    }

    @Override
    protected void updateFunction(BaseEdgeDocument newEdgeDocument, BaseEdgeDocument historyEdgeDocument) {
        super.updateFunction(newEdgeDocument, historyEdgeDocument);
        super.updateDistinctClientIp(newEdgeDocument, historyEdgeDocument);
    }
}
