package cn.ac.iie.etl.relationship;

import cn.ac.iie.etl.update.Relationship;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LocateFqdn2Ip extends Relationship {

    public LocateFqdn2Ip(HashMap<String, HashMap<String, BaseEdgeDocument>> newDocumentHashMap,
                         ArangoDBConnect arangoManger,
                         String collectionName,
                         ConcurrentHashMap<String, BaseEdgeDocument> historyDocumentMap) {
        super(newDocumentHashMap, arangoManger, collectionName, historyDocumentMap);
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
