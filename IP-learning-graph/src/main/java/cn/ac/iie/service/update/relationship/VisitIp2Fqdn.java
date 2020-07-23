package cn.ac.iie.service.update.relationship;

import cn.ac.iie.service.ingestion.ReadClickhouseData;
import cn.ac.iie.service.update.Relationship;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class VisitIp2Fqdn extends Relationship {
    public VisitIp2Fqdn(HashMap<String, ArrayList<BaseEdgeDocument>> newDocumentHashMap,
                        ArangoDBConnect arangoManger,
                        String collectionName,
                        ConcurrentHashMap<String, BaseEdgeDocument> historyDocumentMap,
                        CountDownLatch countDownLatch) {
        super(newDocumentHashMap, arangoManger, collectionName, historyDocumentMap,countDownLatch);
    }

    @Override
    protected void updateFunction(BaseEdgeDocument newEdgeDocument, BaseEdgeDocument historyEdgeDocument) {
        super.updateFunction(newEdgeDocument, historyEdgeDocument);
        for (String schema: ReadClickhouseData.PROTOCOL_SET){
            updateProcotol(historyEdgeDocument,schema,newEdgeDocument);
        }
    }

    @Override
    protected void mergeFunction(BaseEdgeDocument lastDoc,BaseEdgeDocument newDocument) {
        super.mergeFunction(lastDoc, newDocument);
        mergeProtocol(lastDoc,newDocument);
    }
}
