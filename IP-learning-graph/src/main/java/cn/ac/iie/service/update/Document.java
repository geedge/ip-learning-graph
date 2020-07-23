package cn.ac.iie.service.update;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
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

    Document(HashMap<String, ArrayList<T>> newDocumentMap,
             ArangoDBConnect arangoManger,
             String collectionName,
             ConcurrentHashMap<String, T> historyDocumentMap,
             CountDownLatch countDownLatch) {
        this.newDocumentMap = newDocumentMap;
        this.arangoManger = arangoManger;
        this.collectionName = collectionName;
        this.historyDocumentMap = historyDocumentMap;
        this.countDownLatch = countDownLatch;
    }


    @Override
    public void run() {
        long start = System.currentTimeMillis();
        LOG.info("新读取数据"+newDocumentMap.size()+"条，历史数据"+historyDocumentMap.size()+"条");
        try {
            Set<String> keySet = newDocumentMap.keySet();
            ArrayList<T> resultDocumentList = new ArrayList<>();
            int i = 0;
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
            long last = System.currentTimeMillis();
            LOG.info("本线程更新完毕，用时："+(last-start)+"，剩余线程数量："+countDownLatch.getCount());
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

    private T mergeDocument(ArrayList<T> newDocumentSchemaList){
        if (newDocumentSchemaList == null || newDocumentSchemaList.isEmpty()){
            return null;
        }else if (newDocumentSchemaList.size() == 1){
            return newDocumentSchemaList.get(0);
        }else {
            T newDocument = null;
            for (T lastDoc:newDocumentSchemaList){
                if (newDocument == null){
                    newDocument = lastDoc;
                }else {
                    mergeFunction(lastDoc,newDocument);
                }
            }
            return newDocument;
        }
    }

    protected void mergeFunction(T lastDoc,T newDocument) {
        putMinAttribute(lastDoc,newDocument,"FIRST_FOUND_TIME");
        putMaxAttribute(lastDoc,newDocument,"LAST_FOUND_TIME");
    }

    protected void putMinAttribute(T firstDoc,T lastDoc,String attribute){
        long firstMinAttribute = Long.parseLong(firstDoc.getAttribute(attribute).toString());
        long lastMinAttribute = Long.parseLong(lastDoc.getAttribute(attribute).toString());
        lastDoc.addAttribute(attribute,firstMinAttribute<lastMinAttribute? firstMinAttribute:lastMinAttribute);
    }

    protected void putMaxAttribute(T firstDoc,T lastDoc,String attribute){
        long firstMaxAttribute = Long.parseLong(firstDoc.getAttribute(attribute).toString());
        long lastMaxAttribute = Long.parseLong(lastDoc.getAttribute(attribute).toString());
        lastDoc.addAttribute(attribute,firstMaxAttribute>lastMaxAttribute? firstMaxAttribute:lastMaxAttribute);
    }

    protected void putSumAttribute(T firstDoc,T lastDoc,String attribute){
        long firstSumAttribute = Long.parseLong(firstDoc.getAttribute(attribute).toString());
        long lastSumAttribute = Long.parseLong(lastDoc.getAttribute(attribute).toString());
        lastDoc.addAttribute(attribute,firstSumAttribute+lastSumAttribute);
    }


}
