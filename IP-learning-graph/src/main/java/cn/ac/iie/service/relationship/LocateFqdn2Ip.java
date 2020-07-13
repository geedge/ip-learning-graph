package cn.ac.iie.service.relationship;

import cn.ac.iie.service.read.ReadClickhouseData;
import cn.ac.iie.service.update.Relationship;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class LocateFqdn2Ip extends Relationship {

    public LocateFqdn2Ip(HashMap<String, HashMap<String, BaseEdgeDocument>> newDocumentHashMap,
                         ArangoDBConnect arangoManger,
                         String collectionName,
                         ConcurrentHashMap<String, BaseEdgeDocument> historyDocumentMap,
                         CountDownLatch countDownLatch) {
        super(newDocumentHashMap, arangoManger, collectionName, historyDocumentMap,countDownLatch);
    }

    @Override
    protected void mergeFunction(Map<String, Object> properties, BaseEdgeDocument schemaEdgeDoc){
        super.mergeFunction(properties,schemaEdgeDoc);
        mergeDistinctClientIp(properties,schemaEdgeDoc);
    }

    @Override
    protected void updateFunction(BaseEdgeDocument newEdgeDocument, BaseEdgeDocument historyEdgeDocument) {
        super.updateFunction(newEdgeDocument, historyEdgeDocument);
        updateDistinctClientIp(newEdgeDocument, historyEdgeDocument);
    }

    private void mergeDistinctClientIp(Map<String, Object> properties, BaseEdgeDocument schemaEdgeDoc){
        String[] schemaDistCipRecents = (String[]) schemaEdgeDoc.getAttribute("DIST_CIP");
        String[] distCipRecents = (String[]) properties.get("DIST_CIP");
        Object[] mergeClientIp = distinctIp(schemaDistCipRecents, distCipRecents);
        long[] mergeClientIpTs = new long[mergeClientIp.length];
        for (int i = 0;i < mergeClientIpTs.length;i++){
            mergeClientIpTs[i] = ReadClickhouseData.currentHour;
        }
        properties.put("DIST_CIP", mergeClientIp);
        properties.put("DIST_CIP_TS",mergeClientIpTs);
    }

    private void updateDistinctClientIp(BaseEdgeDocument newEdgeDocument,BaseEdgeDocument edgeDocument){
        ArrayList<String> distCip = (ArrayList<String>) edgeDocument.getAttribute("DIST_CIP");
        ArrayList<Long> distCipTs = (ArrayList<Long>) edgeDocument.getAttribute("DIST_CIP_TS");
        HashMap<String, Long> distCipToTs = new HashMap<>();
        if (distCip.size() == distCipTs.size()){
            for (int i = 0;i < distCip.size();i++){
                distCipToTs.put(distCip.get(i),distCipTs.get(i));
            }
        }
        Object[] distCipRecent = (Object[])newEdgeDocument.getAttribute("DIST_CIP");
        for (Object cip:distCipRecent){
            distCipToTs.put(cip.toString(),ReadClickhouseData.currentHour);
        }

        Map<String, Long> sortDistCip = sortMapByValue(distCipToTs);
        edgeDocument.addAttribute("DIST_CIP",sortDistCip.keySet().toArray());
        edgeDocument.addAttribute("DIST_CIP_TS",sortDistCip.values().toArray());
    }


    /**
     * 使用 Map按value进行排序
     */
    private Map<String, Long> sortMapByValue(Map<String, Long> oriMap) {
        if (oriMap == null || oriMap.isEmpty()) {
            return null;
        }
        Map<String, Long> sortedMap = new LinkedHashMap<>();
        List<Map.Entry<String, Long>> entryList = new ArrayList<>(oriMap.entrySet());
        entryList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        if(entryList.size() > 100){
            for(Map.Entry<String, Long> set:entryList.subList(0, 100)){
                sortedMap.put(set.getKey(), set.getValue());
            }
        }else {
            for(Map.Entry<String, Long> set:entryList){
                sortedMap.put(set.getKey(), set.getValue());
            }
        }
        return sortedMap;
    }

    private Object[] distinctIp(Object[] distCipTotalsSrc,Object[] distCipRecentsSrc){
        HashSet<Object> dIpSet = new HashSet<>();
        dIpSet.addAll(Arrays.asList(distCipRecentsSrc));
        dIpSet.addAll(Arrays.asList(distCipTotalsSrc));
        Object[] distCipTotals = dIpSet.toArray();
        if (distCipTotals.length > 100) {
            System.arraycopy(distCipTotals, 0, distCipTotals, 0, 100);
        }
        return distCipTotals;
    }

}
