package cn.ac.iie.service;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class UpdateEIpVisitFqdn implements Runnable {
    private HashMap<String, BaseEdgeDocument> documentHashMap;

    private static final ArangoDBConnect arangoManger = ArangoDBConnect.getInstance();

    public UpdateEIpVisitFqdn(HashMap<String, BaseEdgeDocument> documentHashMap) {
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
                BaseEdgeDocument newEdgeDocument = documentHashMap.getOrDefault(key, null);
                if (newEdgeDocument != null){
                    i += 1;
                    BaseEdgeDocument edgeDocument = BaseArangoData.e_Ip_Visit_Fqdn_Map.getOrDefault(key, null);
                    if (edgeDocument != null){
                        Object lastFoundTime = newEdgeDocument.getAttribute("LAST_FOUND_TIME");
                        long countTotal = Long.parseLong(newEdgeDocument.getAttribute("COUNT_TOTAL").toString());
                        long updateCountTotal = Long.parseLong(edgeDocument.getAttribute("COUNT_TOTAL").toString());
                        edgeDocument.addAttribute("LAST_FOUND_TIME",lastFoundTime);
                        edgeDocument.addAttribute("COUNT_TOTAL",countTotal+updateCountTotal);
                        docInsert.add(edgeDocument);
                    }else {
                        docUpdate.add(newEdgeDocument);
                    }
                }
                if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH){
                    arangoManger.insertAndUpdate(docInsert,docUpdate,"E_VISIT_V_IP_TO_V_FQDN");
                    System.out.println("更新"+i);
                    i = 0;
                }
            }
            if (i != 0){
                arangoManger.insertAndUpdate(docInsert,docUpdate,"E_VISIT_V_IP_TO_V_FQDN");
                System.out.println("更新"+i);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
