package cn.ac.iie.service.update.vertex;

import cn.ac.iie.service.update.Vertex;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class Ip extends Vertex {

    public Ip(HashMap<String, ArrayList<BaseDocument>> newDocumentHashMap,
              ArangoDBConnect arangoManger,
              String collectionName,
              ConcurrentHashMap<String, BaseDocument> historyDocumentMap,
              CountDownLatch countDownLatch) {
        super(newDocumentHashMap, arangoManger, collectionName, historyDocumentMap, countDownLatch);
    }

    @Override
    protected void updateFunction(BaseDocument newDocument, BaseDocument historyDocument) {
        super.updateFunction(newDocument, historyDocument);
        updateIpByType(newDocument, historyDocument);
    }

    @Override
    protected void mergeFunction(BaseDocument lastDoc, BaseDocument newDocument) {
        super.mergeFunction(lastDoc, newDocument);
        mergeIpByType(lastDoc, newDocument);
    }

    private void mergeIpByType(BaseDocument lastDoc, BaseDocument newDocument) {
        putSumAttribute(lastDoc,newDocument,"CLIENT_SESSION_COUNT");
        putSumAttribute(lastDoc,newDocument,"CLIENT_BYTES_SUM");
        putSumAttribute(lastDoc,newDocument,"SERVER_SESSION_COUNT");
        putSumAttribute(lastDoc,newDocument,"SERVER_BYTES_SUM");
    }

    private void updateIpByType(BaseDocument newDocument, BaseDocument historyDocument) {
        putSumAttribute(newDocument, historyDocument, "CLIENT_SESSION_COUNT");
        putSumAttribute(newDocument, historyDocument, "CLIENT_BYTES_SUM");
        putSumAttribute(newDocument, historyDocument, "SERVER_SESSION_COUNT");
        putSumAttribute(newDocument, historyDocument, "SERVER_BYTES_SUM");
    }

}
