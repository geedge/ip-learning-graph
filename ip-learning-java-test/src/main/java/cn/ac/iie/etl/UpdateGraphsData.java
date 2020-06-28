package cn.ac.iie.etl;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.dao.BaseClickhouseData;
import cn.ac.iie.utils.ArangoDBConnect;
import cn.ac.iie.utils.ClickhouseConnect;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;

import java.sql.ResultSet;
import java.util.ArrayList;

public class UpdateGraphsData {

    private static final BaseClickhouseData baseClickhouseData = new BaseClickhouseData();

    private static final ClickhouseConnect clickhouseManger = ClickhouseConnect.getInstance();

    private static final ArangoDBConnect arangoManger = ArangoDBConnect.getInstance();

    public static void updateVFqdn(){
        ResultSet resultSet = baseClickhouseData.BaseRealTimeVFqdn();
        try {
            System.out.println("读取clickhouse成功");
            ArrayList<BaseDocument> docInsert = new ArrayList<>();
            ArrayList<BaseDocument> docUpdate = new ArrayList<>();
            int i = 0;
            System.out.println("开始更新V_FQDN");
            while (resultSet.next()){
                i += 1;
                String fqdnName = resultSet.getString("FQDN_NAME");
                long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                long fqdnCountTotal = resultSet.getLong("FQDN_COUNT_TOTAL");
                BaseDocument document = BaseArangoData.v_Fqdn_Map.getOrDefault(fqdnName, null);
                if (document != null){
                    long countTotal = Long.parseLong(document.getAttribute("FQDN_COUNT_TOTAL").toString());
                    document.addAttribute("LAST_FOUND_TIME",lastFoundTime);
                    document.addAttribute("FQDN_COUNT_TOTAL",countTotal+fqdnCountTotal);
                    docUpdate.add(document);
                }else {
                    BaseDocument newDoc = new BaseDocument();
                    newDoc.setKey(fqdnName);
                    newDoc.addAttribute("FQDN_NAME",fqdnName);
                    newDoc.addAttribute("FIRST_FOUND_TIME",firstFoundTime);
                    newDoc.addAttribute("LAST_FOUND_TIME",lastFoundTime);
                    newDoc.addAttribute("FQDN_COUNT_TOTAL",fqdnCountTotal);
                    docInsert.add(newDoc);
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
        }finally {
            clickhouseManger.clear(baseClickhouseData.pstm,baseClickhouseData.connection);
        }
    }

    public static void updateVIp(){
        ResultSet resultSet = baseClickhouseData.BaseRealTimeVIp();
        try {
            System.out.println("读取clickhouse成功");
            ArrayList<BaseDocument> docInsert = new ArrayList<>();
            ArrayList<BaseDocument> docUpdate = new ArrayList<>();
            int i = 0;
            System.out.println("开始更新V_IP");
            while (resultSet.next()){
                i += 1;
                String ip = resultSet.getString("IP");
                String location = resultSet.getString("location");
                long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                long ipCountTotal = resultSet.getLong("IP_COUNT_TOTAL");
                BaseDocument document = BaseArangoData.v_Ip_Map.getOrDefault(ip, null);
                if (document != null){
                    long countTotal = Long.parseLong(document.getAttribute("IP_COUNT_TOTAL").toString());
                    document.addAttribute("LAST_FOUND_TIME",lastFoundTime);
                    document.addAttribute("IP_COUNT_TOTAL",countTotal+ipCountTotal);
                    docUpdate.add(document);
                }else {
                    BaseDocument newDoc = new BaseDocument();
                    newDoc.setKey(ip);
                    newDoc.addAttribute("IP",ip);
                    newDoc.addAttribute("IP_LOCATION",location);
                    newDoc.addAttribute("FIRST_FOUND_TIME",firstFoundTime);
                    newDoc.addAttribute("LAST_FOUND_TIME",lastFoundTime);
                    newDoc.addAttribute("IP_COUNT_TOTAL",ipCountTotal);
                    docInsert.add(newDoc);
                }
                if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH){
                    arangoManger.insertAndUpdate(docInsert,docUpdate,"V_IP");
                    System.out.println("更新"+i);
                    i = 0;
                }
            }
            if (i != 0){
                arangoManger.insertAndUpdate(docInsert,docUpdate,"V_IP");
                System.out.println("更新"+i);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            clickhouseManger.clear(baseClickhouseData.pstm,baseClickhouseData.connection);
        }
    }

    public static void updateEFqdnAddressIp(){
        ResultSet resultSet = baseClickhouseData.BaseReadTimeEFqdnAddressIp();
        try {
            System.out.println("读取clickhouse成功");
            ArrayList<BaseDocument> docInsert = new ArrayList<>();
            ArrayList<BaseDocument> docUpdate = new ArrayList<>();
            int i = 0;
            System.out.println("开始更新E_ADDRESS_V_FQDN_TO_V_IP");
            while (resultSet.next()){
                i += 1;
                String vFqdn = resultSet.getString("V_FQDN");
                String vIp = resultSet.getString("V_IP");
                long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                long countTotal = resultSet.getLong("COUNT_TOTAL");
                String key = vFqdn+"-"+vIp;
                BaseEdgeDocument edgeDocument = BaseArangoData.e_Fqdn_Address_Ip_Map.getOrDefault(key, null);
                if (edgeDocument != null){
                    long updateCountTotal = Long.parseLong(edgeDocument.getAttribute("COUNT_TOTAL").toString());
                    edgeDocument.addAttribute("LAST_FOUND_TIME",lastFoundTime);
                    edgeDocument.addAttribute("COUNT_TOTAL",countTotal+updateCountTotal);
                    docUpdate.add(edgeDocument);
                }else {
                    BaseEdgeDocument newDoc = new BaseEdgeDocument();
                    newDoc.setKey(key);
                    newDoc.setFrom("V_FQDN/"+vFqdn);
                    newDoc.setTo("V_IP/"+vIp);
                    newDoc.addAttribute("FIRST_FOUND_TIME",firstFoundTime);
                    newDoc.addAttribute("LAST_FOUND_TIME",lastFoundTime);
                    newDoc.addAttribute("COUNT_TOTAL",countTotal);
                    docInsert.add(newDoc);
                }
                if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH){
                    arangoManger.insertAndUpdate(docInsert,docUpdate,"E_ADDRESS_V_FQDN_TO_V_IP");
                    System.out.println("更新"+i);
                    i = 0;
                }
            }
            if (i != 0){
                arangoManger.insertAndUpdate(docInsert,docUpdate,"E_ADDRESS_V_FQDN_TO_V_IP");
                System.out.println("更新"+i);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            clickhouseManger.clear(baseClickhouseData.pstm,baseClickhouseData.connection);
        }
    }

    public static void updateEIpVisitFqdn(){
        ResultSet resultSet = baseClickhouseData.BaseRealTimeEIpVisitFqdn();
        try {
            System.out.println("读取clickhouse成功");
            ArrayList<BaseDocument> docInsert = new ArrayList<>();
            ArrayList<BaseDocument> docUpdate = new ArrayList<>();
            int i = 0;
            System.out.println("开始更新E_VISIT_V_IP_TO_V_FQDN");
            while (resultSet.next()){
                i += 1;
                String vIp = resultSet.getString("V_IP");
                String vFqdn = resultSet.getString("V_FQDN");
                long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                long countTotal = resultSet.getLong("COUNT_TOTAL");
                String key = vIp +"-"+ vFqdn;
                BaseEdgeDocument edgeDocument = BaseArangoData.e_Ip_Visit_Fqdn_Map.getOrDefault(key, null);
                if (edgeDocument != null){
                    long updateCountTotal = Long.parseLong(edgeDocument.getAttribute("COUNT_TOTAL").toString());
                    edgeDocument.addAttribute("LAST_FOUND_TIME",lastFoundTime);
                    edgeDocument.addAttribute("COUNT_TOTAL",countTotal+updateCountTotal);
                    docUpdate.add(edgeDocument);
                }else {
                    BaseEdgeDocument newDoc = new BaseEdgeDocument();
                    newDoc.setKey(key);
                    newDoc.setFrom("V_IP/"+vIp);
                    newDoc.setTo("V_FQDN/"+vFqdn);
                    newDoc.addAttribute("FIRST_FOUND_TIME",firstFoundTime);
                    newDoc.addAttribute("LAST_FOUND_TIME",lastFoundTime);
                    newDoc.addAttribute("COUNT_TOTAL",countTotal);
                    docInsert.add(newDoc);
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
        }finally {
            clickhouseManger.clear(baseClickhouseData.pstm,baseClickhouseData.connection);
        }
    }

}
