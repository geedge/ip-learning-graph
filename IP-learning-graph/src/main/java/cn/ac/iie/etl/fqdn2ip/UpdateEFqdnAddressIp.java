package cn.ac.iie.etl.fqdn2ip;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.etl.BaseUpdateEtl;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class UpdateEFqdnAddressIp implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateEFqdnAddressIp.class);
    private HashMap<String, HashMap<String, BaseEdgeDocument>> documentHashMap;

    private static final ArangoDBConnect arangoManger = ArangoDBConnect.getInstance();

    public UpdateEFqdnAddressIp(HashMap<String, HashMap<String, BaseEdgeDocument>> documentHashMap) {
        this.documentHashMap = documentHashMap;
    }

    @Override
    public void run() {
        Set<String> keySet = documentHashMap.keySet();
        ArrayList<BaseEdgeDocument> docInsert = new ArrayList<>();
        int i = 0;
        try {
            for (String key : keySet) {
                HashMap<String, BaseEdgeDocument> newEdgeDocumentSchmeaMap = documentHashMap.getOrDefault(key, null);
                if (newEdgeDocumentSchmeaMap != null) {
                    BaseEdgeDocument newEdgeDocument = BaseUpdateEtl.mergeFqdn2IpBySchema(newEdgeDocumentSchmeaMap);
                    i += 1;
                    BaseEdgeDocument edgeDocument = BaseArangoData.e_Fqdn_Address_Ip_Map.getOrDefault(key, null);
                    if (edgeDocument != null) {
                        BaseUpdateEtl.mergeFqdn2IpByHistory(newEdgeDocument,edgeDocument);
                        docInsert.add(edgeDocument);
                    } else {
                        docInsert.add(newEdgeDocument);
                    }
                    if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH) {
                        arangoManger.overwrite(docInsert, "R_LOCATE_FQDN2IP");
                        LOG.info("更新R_LOCATE_FQDN2IP:" + i);
                        i = 0;
                    }
                }
            }
            if (i != 0) {
                arangoManger.overwrite(docInsert, "R_LOCATE_FQDN2IP");
                LOG.info("更新R_LOCATE_FQDN2IP:" + i);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
}
