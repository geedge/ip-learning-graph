package cn.ac.iie.dao;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.etl.UpdateEFqdnAddressIp;
import cn.ac.iie.etl.UpdateEIpVisitFqdn;
import cn.ac.iie.etl.UpdateVFqdn;
import cn.ac.iie.etl.UpdateVIP;
import cn.ac.iie.utils.ClickhouseConnect;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

public class BaseClickhouseData {
    private static final Logger LOG = LoggerFactory.getLogger(BaseClickhouseData.class);

    private static final ClickhouseConnect manger = ClickhouseConnect.getInstance();
    private static HashMap<Integer, ArrayList<BaseDocument>> vFqdnMap = new HashMap<>();
    private static HashMap<Integer, ArrayList<BaseDocument>> vIpMap = new HashMap<>();
    private static HashMap<Integer, HashMap<String, BaseEdgeDocument>> eFqdnAddressIpMap = new HashMap<>();
    private static HashMap<Integer, HashMap<String, BaseEdgeDocument>> eIpVisitFqdnMap = new HashMap<>();

    private static long[] getTimeLimit() {
        long maxTime = System.currentTimeMillis() / 1000;
        long minTime = maxTime - 3600;
//        long maxTime = ApplicationConfig.READ_CLICKHOUSE_MAX_TIME;
//        long minTime = ApplicationConfig.READ_CLICKHOUSE_MIN_TIME;
        return new long[]{maxTime, minTime};
    }

    static {
        for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
            vFqdnMap.put(i, new ArrayList<>());
        }
        LOG.info("V_FQDN resultMap初始化完成");

        for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
            vIpMap.put(i, new ArrayList<>());
        }
        LOG.info("V_IP resultMap初始化完成");

        for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
            eFqdnAddressIpMap.put(i, new HashMap<>());
        }
        LOG.info("E_ADDRESS_V_FQDN_TO_V_IP resultMap初始化完成");

        for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
            eIpVisitFqdnMap.put(i, new HashMap<>());
        }
        LOG.info("E_VISIT_V_IP_TO_V_FQDN resultMap初始化完成");
    }

    public static void BaseVFqdn() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = "common_recv_time >= " + minTime + " AND common_recv_time <= " + maxTime + " AND (common_schema_type = 'HTTP' or common_schema_type = 'SSL')";
        String sql = "SELECT common_schema_type,http_host,ssl_sni,MAX(common_recv_time) as LAST_FOUND_TIME,MIN(common_recv_time) as FIRST_FOUND_TIME FROM tsg_galaxy_v3.connection_record_log WHERE " + where + " GROUP BY common_schema_type,http_host,ssl_sni ";
        LOG.info(sql);
        long start = System.currentTimeMillis();
        try {
            DruidPooledConnection connection = manger.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            HashSet<String> fqdnSet = new HashSet<>();
            while (resultSet.next()) {
                String commonSchemaType = resultSet.getString("common_schema_type");
                String fqdnName = commonSchemaGetFqdn(commonSchemaType,resultSet);
                if (!fqdnName.equals("") || !fqdnSet.contains(fqdnName)){
                    fqdnSet.add(fqdnName);
                    long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                    long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                    BaseDocument newDoc = new BaseDocument();
                    newDoc.setKey(fqdnName);
                    newDoc.addAttribute("FQDN_NAME", fqdnName);
                    newDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime);
                    newDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime);
                    int i = Math.abs(fqdnName.hashCode()) % ApplicationConfig.THREAD_POOL_NUMBER;
                    ArrayList<BaseDocument> documentList = vFqdnMap.getOrDefault(i, new ArrayList<>());
                    documentList.add(newDoc);
                }
            }
            long last = System.currentTimeMillis();
            LOG.info("读取clickhouse v_FQDN时间：" + (last - start));
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                ArrayList<BaseDocument> baseDocumentList = vFqdnMap.get(i);
                LOG.info("vFqdn baseDocumentHashMap大小："+baseDocumentList.size());
                UpdateVFqdn updateVFqdn = new UpdateVFqdn(baseDocumentList);
                updateVFqdn.run();
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    public static void BaseVIp() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = " common_recv_time >= " + minTime + " AND common_recv_time <= " + maxTime+ " AND (common_schema_type = 'HTTP' or common_schema_type = 'SSL')";
        String sql = "SELECT IP,location,MIN(common_recv_time) AS FIRST_FOUND_TIME,MAX(common_recv_time) AS LAST_FOUND_TIME,COUNT(*) AS IP_COUNT_TOTAL FROM(( SELECT common_client_ip AS IP, common_client_location AS location, common_recv_time FROM tsg_galaxy_v3.connection_record_log where "+where+" ) UNION ALL ( SELECT common_server_ip AS IP, common_server_location AS location, common_recv_time FROM tsg_galaxy_v3.connection_record_log where "+where+" )) GROUP BY IP,location";
        LOG.info(sql);
        long start = System.currentTimeMillis();
        try {
            DruidPooledConnection connection = manger.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                String ip = resultSet.getString("IP");
                String location = resultSet.getString("location");
                String[] locationSplit = location.split(";");
                String ipLocationNation;
                String ipLocationRegion;
                if (locationSplit.length == 3){
                    ipLocationNation = locationSplit[0];
                    ipLocationRegion = locationSplit[1];
                }else {
                    ipLocationNation = location;
                    ipLocationRegion = location;
                }
                long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                BaseDocument newDoc = new BaseDocument();
                newDoc.setKey(ip);
                newDoc.addAttribute("IP", ip);
                newDoc.addAttribute("IP_LOCATION_NATION", ipLocationNation);
                newDoc.addAttribute("IP_LOCATION_REGION",ipLocationRegion);
                newDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime);
                newDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime);
                int i = Math.abs(ip.hashCode()) % ApplicationConfig.THREAD_POOL_NUMBER;
                ArrayList<BaseDocument> documentList = vIpMap.getOrDefault(i, new ArrayList<>());
                documentList.add(newDoc);
            }
            long last = System.currentTimeMillis();
            LOG.info("读取clickhouse v_IP时间：" + (last - start));
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                ArrayList<BaseDocument> baseDocumentList = vIpMap.get(i);
                LOG.info("vIp baseDocumentHashMap大小："+baseDocumentList.size());
                UpdateVIP updateVIp = new UpdateVIP(baseDocumentList);
                updateVIp.run();
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    public static void BaseEFqdnAddressIp() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = " common_recv_time >= " + minTime + " AND common_recv_time <= " + maxTime+ " AND (common_schema_type = 'HTTP' or common_schema_type = 'SSL')";
        String sql = "SELECT common_schema_type,http_host,ssl_sni,common_server_ip,MAX(common_recv_time) as LAST_FOUND_TIME,MIN(common_recv_time) as FIRST_FOUND_TIME,COUNT(*) as COUNT_TOTAL,groupArray(30)(common_client_ip) as DIST_CIP_RECENT FROM tsg_galaxy_v3.connection_record_log WHERE "+where+" GROUP BY common_schema_type,http_host,ssl_sni,common_server_ip";
        LOG.info(sql);
        long start = System.currentTimeMillis();
        try {
            DruidPooledConnection connection = manger.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            HashMap<String, HashMap<String,Long>> schemaHashMap = new HashMap<>();
//            ArrayList<BaseEdgeDocument> baseEdgeDocuments = new ArrayList<>();

            while (resultSet.next()) {
                String commonSchemaType = resultSet.getString("common_schema_type");
                String vFqdn = commonSchemaGetFqdn(commonSchemaType,resultSet);
                if (!vFqdn.equals("")){
//                String vFqdn = resultSet.getString("http_host");
                    String vIp = resultSet.getString("common_server_ip");
                    long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                    long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                    long countTotal = resultSet.getLong("COUNT_TOTAL");
                    String[] distCipRecents = (String[]) resultSet.getArray("DIST_CIP_RECENT").getArray();
                    String key = vFqdn + "-" + vIp;

                    HashMap<String,Long> map = schemaHashMap.getOrDefault(key, new HashMap<>());
                    Long httpCount = map.getOrDefault(commonSchemaType, 0L);
                    map.put(commonSchemaType,httpCount+countTotal);
                    schemaHashMap.put(key,map);

                    BaseEdgeDocument newDoc = new BaseEdgeDocument();
                    newDoc.setKey(key);
                    newDoc.setFrom("FQDN/" + vFqdn);
                    newDoc.setTo("IP/" + vIp);
                    newDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime);
                    newDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime);
                    newDoc.addAttribute("TLS_CNT_TOTAL", map.getOrDefault("SSL",0L));
                    newDoc.addAttribute("HTTP_CNT_TOTAL", map.getOrDefault("HTTP",0L));
                    newDoc.addAttribute("DIST_CIP_RECENT", distCipRecents);
                    newDoc.addAttribute("DIST_CIP_TOTAL", distCipRecents);
//                baseEdgeDocuments.add(newDoc);
                    int i = Math.abs(key.hashCode()) % ApplicationConfig.THREAD_POOL_NUMBER;
                    HashMap<String, BaseEdgeDocument> documentHashMap = eFqdnAddressIpMap.getOrDefault(i, new HashMap());
                    documentHashMap.put(key, newDoc);
                }
            }
//            ArangoDBConnect.getInstance().insertAndUpdate(baseEdgeDocuments,null,"R_LOCATE_FQDN2IP");
            schemaHashMap.clear();
            long last = System.currentTimeMillis();
            LOG.info("读取clickhouse EFqdnAddressIp时间：" + (last - start));
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, BaseEdgeDocument> baseDocumentHashMap = eFqdnAddressIpMap.get(i);
                LOG.info("EFqdnAddressIp baseDocumentHashMap大小："+baseDocumentHashMap.size());
                UpdateEFqdnAddressIp updateEFqdnAddressIp = new UpdateEFqdnAddressIp(baseDocumentHashMap);
                updateEFqdnAddressIp.run();
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    public static void BaseEIpVisitFqdn() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = " common_recv_time >= " + minTime + " AND common_recv_time <= " + maxTime+ " AND (common_schema_type = 'HTTP' or common_schema_type = 'SSL')";
        String sql = "SELECT common_schema_type,http_host,ssl_sni,common_client_ip,MAX(common_recv_time) as LAST_FOUND_TIME,MIN(common_recv_time) as FIRST_FOUND_TIME,count(*) as COUNT_TOTAL FROM tsg_galaxy_v3.connection_record_log WHERE "+where+" GROUP BY common_schema_type,http_host,ssl_sni,common_client_ip";
        LOG.info(sql);
        long start = System.currentTimeMillis();
        try {
            DruidPooledConnection connection = manger.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            HashMap<String, HashMap<String,Long>> schemaHashMap = new HashMap<>();
            while (resultSet.next()) {
                String commonSchemaType = resultSet.getString("common_schema_type");
                String vIp = resultSet.getString("common_client_ip");
                String vFqdn = commonSchemaGetFqdn(commonSchemaType,resultSet);
                if (!vFqdn.equals("")){
                    String key = vIp +"-"+vFqdn;
                    long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                    long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                    long countTotal = resultSet.getLong("COUNT_TOTAL");
                    HashMap<String,Long> map = schemaHashMap.getOrDefault(key, new HashMap<>());
                    Long httpCount = map.getOrDefault(commonSchemaType, 0L);
                    map.put(commonSchemaType,httpCount+countTotal);
                    schemaHashMap.put(key,map);
                    BaseEdgeDocument newDoc = new BaseEdgeDocument();
                    newDoc.setKey(key);
                    newDoc.setFrom("IP/" + vIp);
                    newDoc.setTo("FQDN/" + vFqdn);
                    newDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime);
                    newDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime);
                    newDoc.addAttribute("TLS_CNT_TOTAL", map.getOrDefault("SSL",0L));
                    newDoc.addAttribute("HTTP_CNT_TOTAL", map.getOrDefault("HTTP",0L));
                    int i = Math.abs(key.hashCode()) % ApplicationConfig.THREAD_POOL_NUMBER;
                    HashMap<String, BaseEdgeDocument> documentHashMap = eIpVisitFqdnMap.getOrDefault(i, new HashMap());
                    documentHashMap.put(key, newDoc);
                }
            }
            schemaHashMap.clear();
            long last = System.currentTimeMillis();
            LOG.info("读取clickhouse EIpVisitFqdn时间：" + (last - start));
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, BaseEdgeDocument> baseDocumentHashMap = eIpVisitFqdnMap.get(i);
                LOG.info("EIpVisitFqdn baseDocumentHashMap大小："+baseDocumentHashMap.size());
                UpdateEIpVisitFqdn updateEIpVisitFqdn = new UpdateEIpVisitFqdn(baseDocumentHashMap);
                updateEIpVisitFqdn.run();
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    private static String commonSchemaGetFqdn(String commonSchemaType,ResultSet resultSet){
        String vFqdn = "";
        try {
            switch (commonSchemaType){
                case "HTTP":
                    vFqdn = resultSet.getString("http_host");
                    break;
                case "SSL":
                    vFqdn = resultSet.getString("ssl_sni");
                    break;
                default:
                    LOG.warn("不支持该类型common_schema_type：" + commonSchemaType);
            }
        }catch (Exception e){
            LOG.error(e.getMessage());
        }
        if (isDomain(vFqdn)){
            return vFqdn;
        }
        return "";
    }

    private static boolean isDomain(String fqdn){
        try {
            String[] fqdnArr = fqdn.split("\\.");
            if (fqdnArr.length < 4 || fqdnArr.length > 4){
                return true;
            }
            Pattern pattern = Pattern.compile("^[\\d]*$");
            for (String f:fqdnArr){
                if (pattern.matcher(f).matches()){
                    int i = Integer.parseInt(f);
                    if (i > 255){
                        return true;
                    }
                }else {
                    return true;
                }
            }
        }catch (Exception e){
            LOG.error("解析域名 "+fqdn+" 失败：\n"+e.toString());
        }
        return false;
    }

}
