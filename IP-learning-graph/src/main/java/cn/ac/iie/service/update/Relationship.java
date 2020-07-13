package cn.ac.iie.service.update;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class Relationship extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(Relationship.class);

    private HashMap<String, HashMap<String, BaseEdgeDocument>> newDocumentHashMap;
    private ArangoDBConnect arangoManger;
    private String collectionName;
    private ConcurrentHashMap<String, BaseEdgeDocument> historyDocumentMap;
    private CountDownLatch countDownLatch;

    public Relationship(HashMap<String, HashMap<String, BaseEdgeDocument>> newDocumentHashMap,
                        ArangoDBConnect arangoManger,
                        String collectionName,
                        ConcurrentHashMap<String, BaseEdgeDocument> historyDocumentMap,
                        CountDownLatch countDownLatch) {
        this.newDocumentHashMap = newDocumentHashMap;
        this.arangoManger = arangoManger;
        this.collectionName = collectionName;
        this.historyDocumentMap = historyDocumentMap;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        Set<String> keySet = newDocumentHashMap.keySet();
        ArrayList<BaseEdgeDocument> docInsert = new ArrayList<>();
        int i = 0;
        try {
            for (String key : keySet) {
                HashMap<String, BaseEdgeDocument> newEdgeDocumentSchemaMap = newDocumentHashMap.getOrDefault(key, null);
                if (newEdgeDocumentSchemaMap != null) {
                    BaseEdgeDocument newEdgeDocument = mergeRelationship(newEdgeDocumentSchemaMap);
                    i += 1;
                    BaseEdgeDocument historyEdgeDocument = historyDocumentMap.getOrDefault(key, null);
                    updateRelationship(newEdgeDocument,historyEdgeDocument,docInsert);
                    if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH) {
                        arangoManger.overwrite(docInsert, collectionName);
                        LOG.info("更新"+collectionName+":" + i);
                        i = 0;
                    }
                }
            }
            if (i != 0) {
                arangoManger.overwrite(docInsert, collectionName);
                LOG.info("更新"+collectionName+":" + i);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.toString());
        }finally {
            countDownLatch.countDown();
        }
    }

    protected BaseEdgeDocument mergeRelationship(HashMap<String, BaseEdgeDocument> newEdgeDocumentSchemaMap) {
        BaseEdgeDocument newBaseEdgeDocument = new BaseEdgeDocument();
        Set<String> schemaSets = newEdgeDocumentSchemaMap.keySet();
        Map<String, Object> properties = newBaseEdgeDocument.getProperties();

        for (String schema : schemaSets) {
            BaseEdgeDocument schemaEdgeDoc = newEdgeDocumentSchemaMap.get(schema);
            if (!properties.isEmpty()) {
                mergeFunction(properties, schemaEdgeDoc);
            } else {
                newBaseEdgeDocument = schemaEdgeDoc;
                properties = schemaEdgeDoc.getProperties();
            }
            setSchemaCount(schema, schemaEdgeDoc, properties);
        }
        properties.remove("COUNT_TOTAL");
        checkSchemaProperty(properties);

        newBaseEdgeDocument.setProperties(properties);
        return newBaseEdgeDocument;
    }

    private void updateRelationship(BaseEdgeDocument newEdgeDocument,BaseEdgeDocument historyEdgeDocument,ArrayList<BaseEdgeDocument> docInsert){
        if (historyEdgeDocument != null && newEdgeDocument != null) {
            updateFunction(newEdgeDocument, historyEdgeDocument);
            docInsert.add(historyEdgeDocument);
        } else {
            docInsert.add(newEdgeDocument);
        }
    }

    protected void updateFunction(BaseEdgeDocument newEdgeDocument,BaseEdgeDocument historyEdgeDocument){
        updateFoundTime(newEdgeDocument,historyEdgeDocument);
        setSchemaCntByHistory(historyEdgeDocument,"TLS_CNT_RECENT","TLS_CNT_TOTAL",newEdgeDocument);
        setSchemaCntByHistory(historyEdgeDocument,"HTTP_CNT_RECENT","HTTP_CNT_TOTAL",newEdgeDocument);
        setSchemaCntByHistory(historyEdgeDocument,"DNS_CNT_RECENT","DNS_CNT_TOTAL",newEdgeDocument);
    }

    protected void updateFoundTime(BaseEdgeDocument newEdgeDocument,BaseEdgeDocument historyEdgeDocument){
        Object lastFoundTime = newEdgeDocument.getAttribute("LAST_FOUND_TIME");
        historyEdgeDocument.addAttribute("LAST_FOUND_TIME", lastFoundTime);
    }

    private void setSchemaCntByHistory(BaseEdgeDocument historyEdgeDocument,String schema,String totalSchema,BaseEdgeDocument newEdgeDocument){
        long countTotal = Long.parseLong(newEdgeDocument.getAttribute(totalSchema).toString());
        long updateCountTotal = Long.parseLong(historyEdgeDocument.getAttribute(totalSchema).toString());

        ArrayList<Long> cntRecent = (ArrayList<Long>) historyEdgeDocument.getAttribute(schema);
        Long[] cntRecentsSrc = cntRecent.toArray(new Long[cntRecent.size()]);
        Long[] cntRecentsDst = new Long[24];
        System.arraycopy(cntRecentsSrc, 0, cntRecentsDst, 1, cntRecentsSrc.length - 1);
        cntRecentsDst[0] = countTotal;

        historyEdgeDocument.addAttribute(schema, cntRecentsDst);
        historyEdgeDocument.addAttribute(totalSchema, countTotal + updateCountTotal);
    }

    protected void mergeFunction(Map<String, Object> properties, BaseEdgeDocument schemaEdgeDoc) {
        mergeFoundTime(properties, schemaEdgeDoc);
    }

    private void mergeFoundTime(Map<String, Object> properties, BaseEdgeDocument schemaEdgeDoc) {
        long schemaFirstFoundTime = Long.parseLong(schemaEdgeDoc.getAttribute("FIRST_FOUND_TIME").toString());
        long firstFoundTime = Long.parseLong(properties.get("FIRST_FOUND_TIME").toString());
        properties.put("FIRST_FOUND_TIME", schemaFirstFoundTime < firstFoundTime ? schemaFirstFoundTime : firstFoundTime);
        long schemaLastFoundTime = Long.parseLong(schemaEdgeDoc.getAttribute("LAST_FOUND_TIME").toString());
        long lastFoundTime = Long.parseLong(properties.get("LAST_FOUND_TIME").toString());
        properties.put("LAST_FOUND_TIME", schemaLastFoundTime > lastFoundTime ? schemaLastFoundTime : lastFoundTime);
    }

    private void setSchemaCount(String schema, BaseEdgeDocument schemaEdgeDoc, Map<String, Object> properties) {
        switch (schema) {
            case "HTTP":
                long httpCntTotal = Long.parseLong(schemaEdgeDoc.getAttribute("COUNT_TOTAL").toString());
                properties.put("HTTP_CNT_TOTAL", httpCntTotal);
                long[] httpCntRecentsDst = new long[24];
                httpCntRecentsDst[0] = httpCntTotal;
                properties.put("HTTP_CNT_RECENT", httpCntRecentsDst);
                break;
            case "SSL":
                long tlsCntTotal = Long.parseLong(schemaEdgeDoc.getAttribute("COUNT_TOTAL").toString());
                properties.put("TLS_CNT_TOTAL", tlsCntTotal);
                long[] tlsCntRecentsDst = new long[24];
                tlsCntRecentsDst[0] = tlsCntTotal;
                properties.put("TLS_CNT_RECENT", tlsCntRecentsDst);
                break;
            default:
                break;
        }
    }

    private void checkSchemaProperty(Map<String, Object> properties){
        if (!properties.containsKey("TLS_CNT_TOTAL")){
            properties.put("TLS_CNT_TOTAL",0L);
            properties.put("TLS_CNT_RECENT",new long[24]);
        }
        if (!properties.containsKey("HTTP_CNT_TOTAL")){
            properties.put("HTTP_CNT_TOTAL",0L);
            properties.put("HTTP_CNT_RECENT",new long[24]);
        }
        if (!properties.containsKey("DNS_CNT_TOTAL")){
            properties.put("DNS_CNT_TOTAL",0L);
            properties.put("DNS_CNT_RECENT",new long[24]);
        }
    }


}
