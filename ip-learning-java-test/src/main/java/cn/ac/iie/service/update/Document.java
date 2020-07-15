package cn.ac.iie.service.update;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class Document<T extends BaseDocument> extends Thread{
    private static final Logger LOG = LoggerFactory.getLogger(Document.class);
    private HashMap<String, ArrayList<T>> newDocumentMap;
    private ArangoDBConnect arangoManger;
    private String collectionName;
    private ConcurrentHashMap<String, T> historyDocumentMap;
    private CountDownLatch countDownLatch;
    private Class<T> type;

    Document(HashMap<String, ArrayList<T>> newDocumentMap,
             ArangoDBConnect arangoManger,
             String collectionName,
             ConcurrentHashMap<String, T> historyDocumentMap,
             CountDownLatch countDownLatch,
             Class<T> type) {
        this.newDocumentMap = newDocumentMap;
        this.arangoManger = arangoManger;
        this.collectionName = collectionName;
        this.historyDocumentMap = historyDocumentMap;
        this.countDownLatch = countDownLatch;
        this.type = type;
    }


    @Override
    public void run() {
        Set<String> keySet = newDocumentMap.keySet();
        ArrayList<T> resultDocumentList = new ArrayList<>();
        int i = 0;
        try {
            for (String key : keySet) {
                ArrayList<T> newDocumentSchemaList = newDocumentMap.getOrDefault(key, null);
                if (newDocumentSchemaList != null) {
                    T newDocument = mergeDocument(newDocumentSchemaList);
                    i += 1;
                    T historyDocument = historyDocumentMap.getOrDefault(key, null);
                    updateDocument(newDocument,historyDocument,resultDocumentList);
                    if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH) {
                        arangoManger.overwrite(resultDocumentList, collectionName);
                        LOG.info("更新"+collectionName+":" + i);
                        i = 0;
                    }
                }
            }
            if (i != 0) {
                arangoManger.overwrite(resultDocumentList, collectionName);
                LOG.info("更新"+collectionName+":" + i);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.toString());
        }finally {
            countDownLatch.countDown();
        }
    }

    private void updateDocument(T newDocument, T historyDocument, ArrayList<T> resultDocumentList) {
        if (historyDocument != null){
            updateFunction(newDocument,historyDocument);
            resultDocumentList.add(historyDocument);
        }else {
            resultDocumentList.add(newDocument);
        }
    }

    protected void updateFunction(T newDocument, T historyDocument) {
        Object lastFoundTime = newDocument.getAttribute("LAST_FOUND_TIME");
        historyDocument.addAttribute("LAST_FOUND_TIME",lastFoundTime);
    }

    private T mergeDocument(ArrayList<T> newDocumentSchemaList) throws IllegalAccessException, InstantiationException {
        if (newDocumentSchemaList == null || newDocumentSchemaList.isEmpty()){
            return null;
        }else if (newDocumentSchemaList.size() == 1){
            return newDocumentSchemaList.get(0);
        }else {
            T document = type.newInstance();
            Map<String, Object> properties = document.getProperties();
            for (T doc:newDocumentSchemaList){
                if (properties.isEmpty()){
                    document = doc;
                    properties = doc.getProperties();
                }else {
                    mergeFunction(properties,doc);
                }
            }
            document.setProperties(properties);
            return document;
        }
    }

    protected void mergeFunction(Map<String, Object> properties, T doc) {
        long firstFoundTime = Long.parseLong(properties.getOrDefault("FIRST_FOUND_TIME", 0L).toString());
        long docFirstFoundTime = Long.parseLong(doc.getAttribute("FIRST_FOUND_TIME").toString());
        properties.put("FIRST_FOUND_TIME",firstFoundTime<docFirstFoundTime? firstFoundTime:docFirstFoundTime);

        long lastFoundTime = Long.parseLong(properties.getOrDefault("LAST_FOUND_TIME", 0L).toString());
        long docLastFoundTime = Long.parseLong(doc.getAttribute("LAST_FOUND_TIME").toString());
        properties.put("LAST_FOUND_TIME",lastFoundTime>docLastFoundTime? lastFoundTime:docLastFoundTime);
    }


}
