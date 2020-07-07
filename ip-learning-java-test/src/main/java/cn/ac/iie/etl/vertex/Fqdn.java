package cn.ac.iie.etl.vertex;

import cn.ac.iie.etl.update.Vertex;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class Fqdn extends Vertex {

    public Fqdn(HashMap<String, ArrayList<BaseDocument>> newDocumentHashMap,
                ArangoDBConnect arangoManger,
                String collectionName,
                ConcurrentHashMap<String, BaseEdgeDocument> historyDocumentMap) {
        super(newDocumentHashMap, arangoManger, collectionName, historyDocumentMap);
    }
}
