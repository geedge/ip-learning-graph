package cn.ac.iie.etl.relationship;

import cn.ac.iie.etl.update.Relationship;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class VisitIp2Fqdn extends Relationship {
    public VisitIp2Fqdn(HashMap<String, HashMap<String, BaseEdgeDocument>> newDocumentHashMap,
                        ArangoDBConnect arangoManger,
                        String collectionName,
                        ConcurrentHashMap<String, BaseEdgeDocument> historyDocumentMap) {
        super(newDocumentHashMap, arangoManger, collectionName, historyDocumentMap);
    }
}
