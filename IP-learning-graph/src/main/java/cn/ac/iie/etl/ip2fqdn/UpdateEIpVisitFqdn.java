package cn.ac.iie.etl.ip2fqdn;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.etl.BaseUpdateEtl;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class UpdateEIpVisitFqdn implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateEIpVisitFqdn.class);
    private HashMap<String, HashMap<String, BaseEdgeDocument>> documentHashMap;

    private static final ArangoDBConnect arangoManger = ArangoDBConnect.getInstance();

    public UpdateEIpVisitFqdn(HashMap<String, HashMap<String, BaseEdgeDocument>> documentHashMap) {
        this.documentHashMap = documentHashMap;
    }

    @Override
    public void run() {
        Set<String> keySet = documentHashMap.keySet();
        ArrayList<BaseEdgeDocument> docInsert = new ArrayList<>();
        int i = 0;
        try {
            for (String key : keySet) {

                HashMap<String, BaseEdgeDocument> newEdgeDocumentMap = documentHashMap.getOrDefault(key, null);
                if (newEdgeDocumentMap != null) {
                    BaseEdgeDocument newEdgeDocument = BaseUpdateEtl.mergeIp2FqdnBySchema(newEdgeDocumentMap);
                    i += 1;
                    BaseEdgeDocument edgeDocument = BaseArangoData.e_Ip_Visit_Fqdn_Map.getOrDefault(key, null);

                    if (edgeDocument != null) {
                        BaseUpdateEtl.mergeIp2FqdnByHistory(newEdgeDocument,edgeDocument);
                        docInsert.add(edgeDocument);
                    } else {
                        docInsert.add(newEdgeDocument);
                    }
                    if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH) {
                        arangoManger.overwrite(docInsert,"R_VISIT_IP2FQDN");
                        LOG.info("更新R_VISIT_IP2FQDN:" + i);
                        i = 0;
                    }
                }
            }
            if (i != 0) {
                arangoManger.overwrite(docInsert,"R_VISIT_IP2FQDN");
                LOG.info("更新R_VISIT_IP2FQDN:" + i);
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }
}
