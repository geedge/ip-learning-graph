package cn.ac.iie.service.update.vertex;

import cn.ac.iie.service.update.Vertex;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class Ip extends Vertex {

    public Ip(HashMap<String, ArrayList<BaseDocument>> newDocumentHashMap,
              ArangoDBConnect arangoManger,
              String collectionName,
              ConcurrentHashMap<String, BaseDocument> historyDocumentMap,
              CountDownLatch countDownLatch) {
        super(newDocumentHashMap, arangoManger, collectionName, historyDocumentMap,countDownLatch);
    }

    @Override
    protected void updateFunction(BaseDocument newDocument, BaseDocument historyDocument) {
        super.updateFunction(newDocument, historyDocument);
        updateIpByType(newDocument, historyDocument);
    }

    @Override
    protected void mergeFunction(Map<String, Object> properties, BaseDocument doc) {
        super.mergeFunction(properties, doc);
        mergeIpByType(properties,doc);
    }

    private void mergeIpByType(Map<String, Object> properties, BaseDocument doc){
        Map<String, Object> mergeProperties = doc.getProperties();
        checkIpTypeProperty(properties,mergeProperties,"CLIENT_SESSION_COUNT");
        checkIpTypeProperty(properties,mergeProperties,"CLIENT_BYTES_SUM");
        checkIpTypeProperty(properties,mergeProperties,"SERVER_SESSION_COUNT");
        checkIpTypeProperty(properties,mergeProperties,"SERVER_BYTES_SUM");
    }

    private void checkIpTypeProperty(Map<String, Object> properties,Map<String, Object> mergeProperties,String property){
        try {
            if (!properties.containsKey(property)){
                properties.put(property,0L);
                checkIpTypeProperty(properties,mergeProperties,property);
            }else if ("0".equals(properties.get(property).toString()) && mergeProperties.containsKey(property)){
                if (!"0".equals(mergeProperties.get(property).toString())){
                    properties.put(property,Long.parseLong(mergeProperties.get(property).toString()));
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void updateIpByType(BaseDocument newDocument, BaseDocument historyDocument){
        addProperty(newDocument,historyDocument,"CLIENT_SESSION_COUNT");
        addProperty(newDocument,historyDocument,"CLIENT_BYTES_SUM");
        addProperty(newDocument,historyDocument,"SERVER_SESSION_COUNT");
        addProperty(newDocument,historyDocument,"SERVER_BYTES_SUM");
    }

    private void addProperty(BaseDocument newDocument, BaseDocument historyDocument,String property){
        try {
            if (historyDocument.getProperties().containsKey(property)){
                long newProperty = Long.parseLong(newDocument.getAttribute(property).toString());
                long hisProperty = Long.parseLong(historyDocument.getAttribute(property).toString());
                historyDocument.updateAttribute(property,newProperty+hisProperty);
            }else {
                historyDocument.addAttribute(property,0L);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
