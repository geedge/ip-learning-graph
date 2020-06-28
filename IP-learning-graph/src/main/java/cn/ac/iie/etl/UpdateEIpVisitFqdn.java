package cn.ac.iie.etl;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

public class UpdateEIpVisitFqdn implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateEIpVisitFqdn.class);
    private HashMap<String, BaseEdgeDocument> documentHashMap;

    private static final ArangoDBConnect arangoManger = ArangoDBConnect.getInstance();

    public UpdateEIpVisitFqdn(HashMap<String, BaseEdgeDocument> documentHashMap) {
        this.documentHashMap = documentHashMap;
    }

    @Override
    public void run() {
        Set<String> keySet = documentHashMap.keySet();
        ArrayList<BaseEdgeDocument> docInsert = new ArrayList<>();
        ArrayList<BaseEdgeDocument> docUpdate = new ArrayList<>();
        int i = 0;
        try {
            for (String key : keySet) {


                BaseEdgeDocument newEdgeDocument = documentHashMap.getOrDefault(key, null);
                if (newEdgeDocument != null) {
                    i += 1;
                    BaseEdgeDocument edgeDocument = BaseArangoData.e_Ip_Visit_Fqdn_Map.getOrDefault(key, null);

                    Object lastFoundTime = newEdgeDocument.getAttribute("LAST_FOUND_TIME");
                    long tlsCountTotal = Long.parseLong(newEdgeDocument.getAttribute("TLS_CNT_TOTAL").toString());
                    long httpCountTotal = Long.parseLong(newEdgeDocument.getAttribute("HTTP_CNT_TOTAL").toString());

                    if (edgeDocument != null) {
                        long tlsUpdateCountTotal = Long.parseLong(edgeDocument.getAttribute("TLS_CNT_TOTAL").toString());
                        long httpUpdateCountTotal = Long.parseLong(edgeDocument.getAttribute("HTTP_CNT_TOTAL").toString());

                        edgeDocument.addAttribute("LAST_FOUND_TIME", lastFoundTime);
                        edgeDocument.addAttribute("TLS_CNT_TOTAL", tlsCountTotal + tlsUpdateCountTotal);
                        edgeDocument.addAttribute("HTTP_CNT_TOTAL", httpCountTotal + httpUpdateCountTotal);

                        ArrayList<Long> tlsCntRecent = (ArrayList<Long>) edgeDocument.getAttribute("TLS_CNT_RECENT");
                        Long[] tlsCntRecentsSrc = tlsCntRecent.toArray(new Long[tlsCntRecent.size()]);
//                        Long[] tlsCntRecentsSrc = (Long[]) edgeDocument.getAttribute("TLS_CNT_RECENT");
                        Long[] tlsCntRecentsDst = new Long[7];
                        System.arraycopy(tlsCntRecentsSrc, 0, tlsCntRecentsDst, 1, tlsCntRecentsSrc.length - 1);
                        tlsCntRecentsDst[0] = tlsCountTotal;
                        edgeDocument.addAttribute("TLS_CNT_RECENT", tlsCntRecentsDst);

                        ArrayList<Long> httpCntRecent = (ArrayList<Long>) edgeDocument.getAttribute("HTTP_CNT_RECENT");
                        Long[] httpCntRecentsSrc = httpCntRecent.toArray(new Long[httpCntRecent.size()]);
//                        Long[] httpCntRecentsSrc = (Long[]) edgeDocument.getAttribute("HTTP_CNT_RECENT");
                        Long[] httpCntRecentsDst = new Long[7];
                        System.arraycopy(httpCntRecentsSrc, 0, httpCntRecentsDst, 1, httpCntRecentsDst.length - 1);
                        httpCntRecentsDst[0] = httpCountTotal;
                        edgeDocument.addAttribute("HTTP_CNT_RECENT", httpCntRecentsDst);

//                        docUpdate.add(edgeDocument);
                        docInsert.add(edgeDocument);
                    } else {
                        long[] tlsCntRecentsDst = new long[7];
                        tlsCntRecentsDst[0] = tlsCountTotal;
                        newEdgeDocument.addAttribute("TLS_CNT_RECENT", tlsCntRecentsDst);

                        long[] httpCntRecentsDst = new long[7];
                        httpCntRecentsDst[0] = httpCountTotal;
                        newEdgeDocument.addAttribute("HTTP_CNT_RECENT", httpCntRecentsDst);

                        docInsert.add(newEdgeDocument);
                    }
                    if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH) {
//                        arangoManger.insertAndUpdate(docInsert, docUpdate, "R_VISIT_IP2FQDN");
                        arangoManger.overwrite(docInsert,"R_VISIT_IP2FQDN");
                        LOG.info("更新R_VISIT_IP2FQDN:" + i);
                        i = 0;
                    }
                }
            }
            if (i != 0) {
//                arangoManger.insertAndUpdate(docInsert, docUpdate, "R_VISIT_IP2FQDN");
                arangoManger.overwrite(docInsert,"R_VISIT_IP2FQDN");
                LOG.info("更新R_VISIT_IP2FQDN:" + i);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
}
