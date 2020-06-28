package cn.ac.iie.etl;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class UpdateEFqdnAddressIp implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateEFqdnAddressIp.class);
    private HashMap<String, BaseEdgeDocument> documentHashMap;

    private static final ArangoDBConnect arangoManger = ArangoDBConnect.getInstance();

    public UpdateEFqdnAddressIp(HashMap<String, BaseEdgeDocument> documentHashMap) {
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
                    BaseEdgeDocument edgeDocument = BaseArangoData.e_Fqdn_Address_Ip_Map.getOrDefault(key, null);

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

                        ArrayList<String> distCipTotal = (ArrayList<String>) edgeDocument.getAttribute("DIST_CIP_TOTAL");
                        String[] distCipTotalsSrc = distCipTotal.toArray(new String[distCipTotal.size()]);
//                        String[] distCipTotalsSrc = (String[]) edgeDocument.getAttribute("DIST_CIP_TOTAL");

//                        ArrayList<String> distCipRecent = (ArrayList<String>) newEdgeDocument.getAttribute("DIST_CIP_RECENT");
//                        String[] distCipRecentsSrc = distCipRecent.toArray(new String[distCipRecent.size()]);
                        String[] distCipRecentsSrc = (String[]) newEdgeDocument.getAttribute("DIST_CIP_RECENT");

                        if (distCipTotalsSrc.length == 30) {
                            HashSet<String> dIpSet = new HashSet<>();
                            dIpSet.addAll(Arrays.asList(distCipRecentsSrc));
                            dIpSet.addAll(Arrays.asList(distCipTotalsSrc));
                            Object[] distCipTotals = dIpSet.toArray();
                            if (distCipTotals.length > 30) {
                                System.arraycopy(distCipTotals, 0, distCipTotals, 0, 30);
                            }
                            edgeDocument.addAttribute("DIST_CIP_TOTAL", distCipTotals);
                        }
                        edgeDocument.addAttribute("DIST_CIP_RECENT", distCipRecentsSrc);

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
//                        arangoManger.insertAndUpdate(docInsert, docUpdate, "R_LOCATE_FQDN2IP");
                        arangoManger.overwrite(docInsert,"R_LOCATE_FQDN2IP");
                        LOG.info("更新R_LOCATE_FQDN2IP:" + i);
                        i = 0;
                    }
                }
            }
            if (i != 0) {
//                arangoManger.insertAndUpdate(docInsert, docUpdate, "R_LOCATE_FQDN2IP");
                arangoManger.overwrite(docInsert,"R_LOCATE_FQDN2IP");
                LOG.info("更新R_LOCATE_FQDN2IP:" + i);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
}
