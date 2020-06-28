package cn.ac.iie.etl;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class UpdateVFqdn implements Runnable{

    private HashMap<String, BaseDocument> documentHashMap;

    private static final ArangoDBConnect arangoManger = ArangoDBConnect.getInstance();

    public UpdateVFqdn(HashMap<String, BaseDocument> documentHashMap) {
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
                BaseDocument newDocument = documentHashMap.getOrDefault(key, null);
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
}
