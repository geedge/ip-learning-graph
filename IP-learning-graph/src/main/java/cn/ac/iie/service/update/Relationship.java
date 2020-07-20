package cn.ac.iie.service.update;

import cn.ac.iie.service.ingestion.ReadClickhouseData;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class Relationship extends Document<BaseEdgeDocument> {

    public Relationship(HashMap<String, ArrayList<BaseEdgeDocument>> newDocumentHashMap,
                        ArangoDBConnect arangoManger,
                        String collectionName,
                        ConcurrentHashMap<String, BaseEdgeDocument> historyDocumentMap,
                        CountDownLatch countDownLatch) {
        super(newDocumentHashMap,arangoManger,collectionName,historyDocumentMap,countDownLatch,BaseEdgeDocument.class);
    }

    @Override
    protected void updateFunction(BaseEdgeDocument newEdgeDocument, BaseEdgeDocument historyEdgeDocument){
        super.updateFunction(newEdgeDocument,historyEdgeDocument);
    }

    protected void updateProcotol(BaseEdgeDocument historyEdgeDocument, String schema, BaseEdgeDocument newEdgeDocument){
        String recentSchema = schema +"_CNT_RECENT";
        String totalSchema = schema + "_CNT_TOTAL";
        long countTotal = Long.parseLong(newEdgeDocument.getAttribute(totalSchema).toString());
        if (countTotal > 0L){
            long updateCountTotal = Long.parseLong(historyEdgeDocument.getAttribute(totalSchema).toString());

            Long[] cntRecent = (Long[]) historyEdgeDocument.getAttribute(recentSchema);
            cntRecent[0] = countTotal;

            historyEdgeDocument.addAttribute(recentSchema, cntRecent);
            historyEdgeDocument.addAttribute(totalSchema, countTotal + updateCountTotal);
            String hisProtocolType = historyEdgeDocument.getAttribute("PROTOCOL_TYPE").toString();
            if (!hisProtocolType.contains(schema)){
                hisProtocolType = hisProtocolType + "," + schema;
                historyEdgeDocument.addAttribute("PROTOCOL_TYPE",hisProtocolType);
            }
        }
    }

    @Override
    protected void mergeFunction(Map<String, Object> newProperties, BaseEdgeDocument lastDoc) {
        super.mergeFunction(newProperties, lastDoc);
    }

    protected void mergeProtocol(Map<String, Object> newProperties, BaseEdgeDocument lastDoc) {
        String schema = lastDoc.getAttribute("PROTOCOL_TYPE").toString();
        if (ReadClickhouseData.protocolSet.contains(schema)){
            setProtocolProperties(schema,newProperties,lastDoc);
        }
    }

    private void setProtocolProperties(String protocol,Map<String, Object> newProperties, BaseEdgeDocument lastDoc){
        String protocolRecent = protocol +"_CNT_RECENT";
        String protocolTotal = protocol + "_CNT_TOTAL";
        long httpCntTotal = Long.parseLong(lastDoc.getAttribute(protocolTotal).toString());
        newProperties.put(protocolTotal, httpCntTotal);
        long[] httpCntRecents = (long[]) lastDoc.getAttribute(protocolRecent);
        newProperties.put(protocolRecent, httpCntRecents);
        String protocolType = newProperties.get("PROTOCOL_TYPE").toString();
        newProperties.put("PROTOCOL_TYPE",addProcotolType(protocolType,protocol));
    }

    private String addProcotolType(String protocolType,String schema){
        if (!protocolType.contains(schema)){
            protocolType = protocolType + "," + schema;
        }
        return protocolType;
    }

}
