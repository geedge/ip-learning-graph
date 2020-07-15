package cn.ac.iie.service;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class UpdateVFqdn implements Runnable{

    private HashMap<String,  ArrayList<BaseDocument>> documentHashMap;

    private static ArangoDBConnect arangoManger = ArangoDBConnect.getInstance();

    public UpdateVFqdn(HashMap<String,  ArrayList<BaseDocument>> documentHashMap) {
        this.documentHashMap = documentHashMap;
    }

    @Override
    public void run() {
        Set<String> keySet = documentHashMap.keySet();
        ArrayList<BaseDocument> docInsert = new ArrayList<>();
        ArrayList<BaseDocument> docUpdate = new ArrayList<>();
        int i = 0;
        try {
            for (String key:keySet){
                ArrayList<BaseDocument> documentArrayList = documentHashMap.getOrDefault(key, null);
                BaseDocument newDocument = mergeVFqdn(documentArrayList);

                if (newDocument != null){
                    i += 1;
                    BaseDocument document = BaseArangoData.v_Fqdn_Map.getOrDefault(key, null);
                    if (document != null){
                        Object lastFoundTime = newDocument.getAttribute("LAST_FOUND_TIME");
                        long fqdnCountTotal = Long.parseLong(newDocument.getAttribute("FQDN_COUNT_TOTAL").toString());
                        long countTotal = Long.parseLong(document.getAttribute("FQDN_COUNT_TOTAL").toString());
                        document.addAttribute("LAST_FOUND_TIME",lastFoundTime);
                        document.addAttribute("FQDN_COUNT_TOTAL",countTotal+fqdnCountTotal);
                        docUpdate.add(document);
                    }else {
                        docInsert.add(newDocument);
                    }
                }
                if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH){
                    arangoManger.insertAndUpdate(docInsert,docUpdate,"V_FQDN");
                    System.out.println("更新"+i);
                    i = 0;
                }
            }
            if (i != 0){
                arangoManger.insertAndUpdate(docInsert,docUpdate,"V_FQDN");
                System.out.println("更新"+i);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private BaseDocument mergeVFqdn(ArrayList<BaseDocument> documentArrayList){
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
                    long firstFoundTime = Long.parseLong(properties.getOrDefault("FIRST_FOUND_TIME", 0L).toString());
                    long docFirstFoundTime = Long.parseLong(doc.getAttribute("FIRST_FOUND_TIME").toString());
                    properties.put("FIRST_FOUND_TIME",firstFoundTime<docFirstFoundTime? firstFoundTime:docFirstFoundTime);

                    long lastFoundTime = Long.parseLong(properties.getOrDefault("LAST_FOUND_TIME", 0L).toString());
                    long docLastFoundTime = Long.parseLong(doc.getAttribute("LAST_FOUND_TIME").toString());
                    properties.put("LAST_FOUND_TIME",lastFoundTime>docLastFoundTime? lastFoundTime:docLastFoundTime);

                    long fqdnCountTotal = Long.parseLong(properties.getOrDefault("FQDN_COUNT_TOTAL", 0L).toString());
                    long docFqdnCountTotal = Long.parseLong(doc.getAttribute("FQDN_COUNT_TOTAL").toString());
                    properties.put("FQDN_COUNT_TOTAL",fqdnCountTotal+docFqdnCountTotal);
                }
            }
            document.setProperties(properties);
            return document;
        }
    }
}
