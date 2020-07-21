package cn.ac.iie.service.update.relationship;

import cn.ac.iie.service.update.Relationship;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class SameFqdn2Fqdn extends Relationship {

    public SameFqdn2Fqdn(HashMap<String, ArrayList<BaseEdgeDocument>> newDocumentHashMap,
                         ArangoDBConnect arangoManger,
                         String collectionName,
                         ConcurrentHashMap<String, BaseEdgeDocument> historyDocumentMap,
                         CountDownLatch countDownLatch) {
        super(newDocumentHashMap, arangoManger, collectionName, historyDocumentMap, countDownLatch);
    }

    @Override
    protected void updateFunction(BaseEdgeDocument newEdgeDocument, BaseEdgeDocument historyEdgeDocument) {
        super.updateFunction(newEdgeDocument, historyEdgeDocument);
        putSumAttribute(newEdgeDocument,historyEdgeDocument,"CNT_TOTAL");
    }

    @Override
    protected void mergeFunction(BaseEdgeDocument lastDoc,BaseEdgeDocument newDocument) {
        super.mergeFunction(lastDoc, newDocument);
        putSumAttribute(lastDoc,newDocument,"CNT_TOTAL");
    }

}
