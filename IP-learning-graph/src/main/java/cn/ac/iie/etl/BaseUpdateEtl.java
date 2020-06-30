package cn.ac.iie.etl;

import com.arangodb.entity.BaseEdgeDocument;

import java.util.*;

public class BaseUpdateEtl {

    public static BaseEdgeDocument mergeFqdn2IpBySchema(HashMap<String, BaseEdgeDocument> newEdgeDocumentSchemaMap){

        BaseEdgeDocument newBaseEdgeDocument = new BaseEdgeDocument();
        Set<String> schemaSets = newEdgeDocumentSchemaMap.keySet();
        for (String schema : schemaSets) {
            BaseEdgeDocument schemaEdgeDoc = newEdgeDocumentSchemaMap.get(schema);
            setSchemaCnt(schema,schemaEdgeDoc,newBaseEdgeDocument);
            if (newBaseEdgeDocument.getKey() != null){
                Map<String, Object> properties = newBaseEdgeDocument.getProperties();
                setFoundTime(properties,schemaEdgeDoc);
                setDistinctClientIpBySchema(properties,schemaEdgeDoc);
            }else {
                Map<String, Object> properties = schemaEdgeDoc.getProperties();
                properties.remove("COUNT_TOTAL");
                newBaseEdgeDocument = schemaEdgeDoc;
            }
        }
        return newBaseEdgeDocument;
    }

    public static BaseEdgeDocument mergeIp2FqdnBySchema(HashMap<String, BaseEdgeDocument> newEdgeDocumentMap){
        BaseEdgeDocument newBaseEdgeDocument = new BaseEdgeDocument();
        Set<String> schemaSets = newEdgeDocumentMap.keySet();
        for (String schema : schemaSets) {
            BaseEdgeDocument schemaEdgeDoc = newEdgeDocumentMap.get(schema);
            setSchemaCnt(schema,schemaEdgeDoc,newBaseEdgeDocument);
            if (newBaseEdgeDocument.getKey() != null){
                Map<String, Object> properties = newBaseEdgeDocument.getProperties();
                setFoundTime(properties,schemaEdgeDoc);
            }else {
                Map<String, Object> properties = schemaEdgeDoc.getProperties();
                properties.remove("COUNT_TOTAL");
                newBaseEdgeDocument = schemaEdgeDoc;
            }
        }
        return newBaseEdgeDocument;
    }

    public static void mergeIp2FqdnByHistory(BaseEdgeDocument newEdgeDocument,BaseEdgeDocument edgeDocument){
        updateCommonProperty(newEdgeDocument,edgeDocument);
    }

    public static void mergeFqdn2IpByHistory(BaseEdgeDocument newEdgeDocument,BaseEdgeDocument edgeDocument){
        updateCommonProperty(newEdgeDocument,edgeDocument);
        setDistinctClientIpByHistory(newEdgeDocument,edgeDocument);
    }

    private static void setDistinctClientIpByHistory(BaseEdgeDocument newEdgeDocument,BaseEdgeDocument edgeDocument){
        ArrayList<String> distCipTotal = (ArrayList<String>) edgeDocument.getAttribute("DIST_CIP_TOTAL");
        String[] distCipTotalsSrc = distCipTotal.toArray(new String[distCipTotal.size()]);

        String[] distCipRecentsSrc = (String[]) newEdgeDocument.getAttribute("DIST_CIP_RECENT");
        if (distCipTotalsSrc.length == 30) {
            Object[] distCipTotals = mergeClientIp(distCipTotalsSrc, distCipRecentsSrc);
            edgeDocument.addAttribute("DIST_CIP_TOTAL", distCipTotals);
        }
        edgeDocument.addAttribute("DIST_CIP_RECENT", distCipRecentsSrc);
    }

    private static void updateCommonProperty(BaseEdgeDocument newEdgeDocument,BaseEdgeDocument edgeDocument){
        Object lastFoundTime = newEdgeDocument.getAttribute("LAST_FOUND_TIME");
        edgeDocument.addAttribute("LAST_FOUND_TIME", lastFoundTime);

        setSchemaCntByHistory(edgeDocument,"TLS_CNT_RECENT","TLS_CNT_TOTAL",newEdgeDocument);
        setSchemaCntByHistory(edgeDocument,"HTTP_CNT_RECENT","HTTP_CNT_TOTAL",newEdgeDocument);

    }

    private static void setSchemaCntByHistory(BaseEdgeDocument edgeDocument,String schema,String totalSchema,BaseEdgeDocument newEdgeDocument){
        long countTotal = Long.parseLong(newEdgeDocument.getAttribute(totalSchema).toString());
        long updateCountTotal = Long.parseLong(edgeDocument.getAttribute(totalSchema).toString());

        ArrayList<Long> cntRecent = (ArrayList<Long>) edgeDocument.getAttribute(schema);
        Long[] cntRecentsSrc = cntRecent.toArray(new Long[cntRecent.size()]);
        Long[] cntRecentsDst = new Long[7];
        System.arraycopy(cntRecentsSrc, 0, cntRecentsDst, 1, cntRecentsSrc.length - 1);
        cntRecentsDst[0] = countTotal;

        edgeDocument.addAttribute(schema, cntRecentsDst);
        edgeDocument.addAttribute(totalSchema, countTotal + updateCountTotal);
    }

    private static Object[] mergeClientIp(String[] distCipTotalsSrc,String[] distCipRecentsSrc){
        HashSet<String> dIpSet = new HashSet<>();
        dIpSet.addAll(Arrays.asList(distCipRecentsSrc));
        dIpSet.addAll(Arrays.asList(distCipTotalsSrc));
        Object[] distCipTotals = dIpSet.toArray();
        if (distCipTotals.length > 30) {
            System.arraycopy(distCipTotals, 0, distCipTotals, 0, 30);
        }
        return distCipTotals;
    }

    private static void setDistinctClientIpBySchema(Map<String, Object> properties,BaseEdgeDocument schemaEdgeDoc){
        String[] schemaDistCipRecents = (String[]) schemaEdgeDoc.getAttribute("DIST_CIP_RECENT");
        String[] distCipRecents = (String[]) properties.get("DIST_CIP_RECENT");
        Object[] mergeClientIp = mergeClientIp(schemaDistCipRecents, distCipRecents);
        properties.put("DIST_CIP_RECENT",mergeClientIp);
        properties.put("DIST_CIP_TOTAL",mergeClientIp);
    }

    private static void setFoundTime(Map<String, Object> properties,BaseEdgeDocument schemaEdgeDoc){
        long schemaFirstFoundTime = Long.parseLong(schemaEdgeDoc.getAttribute("FIRST_FOUND_TIME").toString());
        long firstFoundTime = Long.parseLong(properties.get("FIRST_FOUND_TIME").toString());
        properties.put("FIRST_FOUND_TIME",schemaFirstFoundTime<firstFoundTime?schemaFirstFoundTime:firstFoundTime);
        long schemaLastFoundTime = Long.parseLong(schemaEdgeDoc.getAttribute("LAST_FOUND_TIME").toString());
        long lastFoundTime = Long.parseLong(properties.get("LAST_FOUND_TIME").toString());
        properties.put("LAST_FOUND_TIME",schemaLastFoundTime>lastFoundTime?schemaLastFoundTime:lastFoundTime);
    }

    private static void setSchemaCnt(String schema,BaseEdgeDocument schemaEdgeDoc,BaseEdgeDocument newBaseEdgeDocument){
        switch (schema) {
            case "HTTP":
                long httpCntTotal = Long.parseLong(schemaEdgeDoc.getAttribute("COUNT_TOTAL").toString());
                newBaseEdgeDocument.addAttribute("HTTP_CNT_TOTAL", httpCntTotal);
                long[] httpCntRecentsDst = new long[7];
                httpCntRecentsDst[0] = httpCntTotal;
                newBaseEdgeDocument.addAttribute("HTTP_CNT_RECENT", httpCntRecentsDst);
                break;
            case "SSL":
                long tlsCntTotal = Long.parseLong(schemaEdgeDoc.getAttribute("COUNT_TOTAL").toString());
                newBaseEdgeDocument.addAttribute("TLS_CNT_TOTAL", tlsCntTotal);
                long[] tlsCntRecentsDst = new long[7];
                tlsCntRecentsDst[0] = tlsCntTotal;
                newBaseEdgeDocument.addAttribute("TLS_CNT_RECENT", tlsCntRecentsDst);
                break;
        }
    }

}
