package cn.ac.iie.etl.update;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wlh
 * 多线程更新vertex数据
 */
public class Vertex extends Thread{

    protected HashMap<String,  ArrayList<BaseDocument>> newDocumentHashMap;
    protected ArangoDBConnect arangoManger;
    protected String collectionName;
    protected ConcurrentHashMap<String, BaseEdgeDocument> historyDocumentMap;

    public Vertex(HashMap<String,  ArrayList<BaseDocument>> newDocumentHashMap,
                  ArangoDBConnect arangoManger,
                  String collectionName,
                  ConcurrentHashMap<String, BaseEdgeDocument> historyDocumentMap){
        this.newDocumentHashMap = newDocumentHashMap;
        this.arangoManger = arangoManger;
        this.collectionName = collectionName;
        this.historyDocumentMap = historyDocumentMap;
    }

    @Override
    public void run() {
        Set<String> keySet = newDocumentHashMap.keySet();
        ArrayList<BaseDocument> docInsert = new ArrayList<>();
        int i = 0;
        try {
            for (String key:keySet){
                ArrayList<BaseDocument> documentArrayList = newDocumentHashMap.getOrDefault(key, null);
                BaseDocument newDocument = mergeVertex(documentArrayList);
                if (newDocument != null){
                    i += 1;
                    BaseDocument historyDocument = historyDocumentMap.getOrDefault(key, null);
                    updateVertex(newDocument,historyDocument,docInsert);
                }
                if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH){
                    arangoManger.overwrite(docInsert,collectionName);
                    System.out.println("更新"+i);
                    i = 0;
                }
            }
            if (i != 0){
                arangoManger.overwrite(docInsert,collectionName);
                System.out.println("更新"+i);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void updateVertex(BaseDocument newDocument,BaseDocument historyDocument,ArrayList<BaseDocument> docInsert){
        if (historyDocument != null){
            updateFunction(newDocument,historyDocument);
            docInsert.add(historyDocument);
        }else {
            docInsert.add(newDocument);
        }
    }

    protected void updateFunction(BaseDocument newDocument,BaseDocument historyDocument){
        updateFoundTime(newDocument,historyDocument);
    }

    private void updateFoundTime(BaseDocument newDocument,BaseDocument historyDocument){
        Object lastFoundTime = newDocument.getAttribute("LAST_FOUND_TIME");
        historyDocument.addAttribute("LAST_FOUND_TIME",lastFoundTime);
    }

    private BaseDocument mergeVertex(ArrayList<BaseDocument> documentArrayList){
        if (documentArrayList == null || documentArrayList.isEmpty()){
            return null;
        }else if (documentArrayList.size() == 1){
            return documentArrayList.get(0);
        }else {
            BaseDocument document = new BaseDocument();
            Map<String, Object> properties = document.getProperties();
            for (BaseDocument doc:documentArrayList){
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

    protected void mergeFunction(Map<String, Object> properties,BaseDocument doc){
        mergeFoundTime(properties,doc);
    }

    private void mergeFoundTime(Map<String, Object> properties,BaseDocument doc){
        long firstFoundTime = Long.parseLong(properties.getOrDefault("FIRST_FOUND_TIME", 0L).toString());
        long docFirstFoundTime = Long.parseLong(doc.getAttribute("FIRST_FOUND_TIME").toString());
        properties.put("FIRST_FOUND_TIME",firstFoundTime<docFirstFoundTime? firstFoundTime:docFirstFoundTime);

        long lastFoundTime = Long.parseLong(properties.getOrDefault("LAST_FOUND_TIME", 0L).toString());
        long docLastFoundTime = Long.parseLong(doc.getAttribute("LAST_FOUND_TIME").toString());
        properties.put("LAST_FOUND_TIME",lastFoundTime>docLastFoundTime? lastFoundTime:docLastFoundTime);
    }
}
