package cn.ac.iie.dao;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.etl.fqdn2ip.UpdateEFqdnAddressIp;
import cn.ac.iie.etl.ip2fqdn.UpdateEIpVisitFqdn;
import cn.ac.iie.etl.fqdn.UpdateVFqdn;
import cn.ac.iie.etl.ip.UpdateVIP;
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
    private static HashMap<Integer, HashMap<String, HashMap<String, BaseEdgeDocument>>> eFqdnAddressIpMap = new HashMap<>();
    private static HashMap<Integer, HashMap<String, HashMap<String, BaseEdgeDocument>>> eIpVisitFqdnMap = new HashMap<>();

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

        String sql = getVFqdnSql();
        long start = System.currentTimeMillis();
        try {
            DruidPooledConnection connection = manger.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
//            HashSet<String> fqdnSet = new HashSet<>();
            while (resultSet.next()) {
//                String commonSchemaType = resultSet.getString("common_schema_type");
//                String fqdnName = commonSchemaGetFqdn(commonSchemaType,resultSet);
                String fqdnName = resultSet.getString("FQDN");
//                fqdnSet.add(fqdnName);
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
            long last = System.currentTimeMillis();
            LOG.info(sql + "\n读取clickhouse v_FQDN时间：" + (last - start));
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                ArrayList<BaseDocument> baseDocumentList = vFqdnMap.get(i);
                LOG.info("vFqdn baseDocumentHashMap大小：" + baseDocumentList.size());
                UpdateVFqdn updateVFqdn = new UpdateVFqdn(baseDocumentList);
                updateVFqdn.run();
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    public static void BaseVIp() {
        String sql = getVIpSql();
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
                if (locationSplit.length == 3) {
                    ipLocationNation = locationSplit[0];
                    ipLocationRegion = locationSplit[1];
                } else {
                    ipLocationNation = location;
                    ipLocationRegion = location;
                }
                long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                BaseDocument newDoc = new BaseDocument();
                newDoc.setKey(ip);
                newDoc.addAttribute("IP", ip);
                newDoc.addAttribute("IP_LOCATION_NATION", ipLocationNation);
                newDoc.addAttribute("IP_LOCATION_REGION", ipLocationRegion);
                newDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime);
                newDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime);
                int i = Math.abs(ip.hashCode()) % ApplicationConfig.THREAD_POOL_NUMBER;
                ArrayList<BaseDocument> documentList = vIpMap.getOrDefault(i, new ArrayList<>());
                documentList.add(newDoc);
            }
            long last = System.currentTimeMillis();
            LOG.info(sql + "\n读取clickhouse v_IP时间：" + (last - start));
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                ArrayList<BaseDocument> baseDocumentList = vIpMap.get(i);
                LOG.info("vIp baseDocumentHashMap大小：" + baseDocumentList.size());
                UpdateVIP updateVIp = new UpdateVIP(baseDocumentList);
                updateVIp.run();
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    public static void BaseEFqdnAddressIp() {

        String sql = getEFqdnAddressIpSql();
        long start = System.currentTimeMillis();
        try {
            DruidPooledConnection connection = manger.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                String commonSchemaType = resultSet.getString("common_schema_type");
                String vFqdn = resultSet.getString("FQDN");
                String vIp = resultSet.getString("common_server_ip");
                long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                long countTotal = resultSet.getLong("COUNT_TOTAL");
                String[] distCipRecents = (String[]) resultSet.getArray("DIST_CIP_RECENT").getArray();

                String key = vFqdn + "-" + vIp;
                BaseEdgeDocument newDoc = new BaseEdgeDocument();
                newDoc.setKey(key);
                newDoc.setFrom("FQDN/" + vFqdn);
                newDoc.setTo("IP/" + vIp);
                newDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime);
                newDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime);
                newDoc.addAttribute("COUNT_TOTAL", countTotal);
                newDoc.addAttribute("DIST_CIP_RECENT", distCipRecents);
                newDoc.addAttribute("DIST_CIP_TOTAL", distCipRecents);

                int hashMod = Math.abs(key.hashCode()) % ApplicationConfig.THREAD_POOL_NUMBER;
                HashMap<String, HashMap<String, BaseEdgeDocument>> documentHashMap = eFqdnAddressIpMap.getOrDefault(hashMod, new HashMap());

                HashMap<String, BaseEdgeDocument> schemaHashMap = documentHashMap.getOrDefault(key, new HashMap<>());
                schemaHashMap.put(commonSchemaType, newDoc);
                documentHashMap.put(key, schemaHashMap);
            }
            long last = System.currentTimeMillis();
            LOG.info(sql + "\n读取clickhouse EFqdnAddressIp时间：" + (last - start));
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, HashMap<String, BaseEdgeDocument>> baseDocumentHashMap = eFqdnAddressIpMap.get(i);
                LOG.info("EFqdnAddressIp baseDocumentHashMap大小：" + baseDocumentHashMap.size());
                UpdateEFqdnAddressIp updateEFqdnAddressIp = new UpdateEFqdnAddressIp(baseDocumentHashMap);
                updateEFqdnAddressIp.run();
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    public static void BaseEIpVisitFqdn() {
        String sql = getEIpVisitFqdnSql();
        long start = System.currentTimeMillis();
        try {
            DruidPooledConnection connection = manger.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                String commonSchemaType = resultSet.getString("common_schema_type");
                String vIp = resultSet.getString("common_client_ip");
                String vFqdn = resultSet.getString("FQDN");
                String key = vIp + "-" + vFqdn;
                long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                long countTotal = resultSet.getLong("COUNT_TOTAL");

                BaseEdgeDocument newDoc = new BaseEdgeDocument();
                newDoc.setKey(key);
                newDoc.setFrom("IP/" + vIp);
                newDoc.setTo("FQDN/" + vFqdn);
                newDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime);
                newDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime);
                newDoc.addAttribute("COUNT_TOTAL", countTotal);
                int i = Math.abs(key.hashCode()) % ApplicationConfig.THREAD_POOL_NUMBER;
                HashMap<String, HashMap<String, BaseEdgeDocument>> documentHashMap = eIpVisitFqdnMap.getOrDefault(i, new HashMap());

                HashMap<String, BaseEdgeDocument> schemaHashMap = documentHashMap.getOrDefault(key, new HashMap<>());
                schemaHashMap.put(commonSchemaType, newDoc);
                documentHashMap.put(key, schemaHashMap);
            }
            long last = System.currentTimeMillis();
            LOG.info(sql + "\n读取clickhouse EIpVisitFqdn时间：" + (last - start));
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                HashMap<String, HashMap<String, BaseEdgeDocument>> baseDocumentHashMap = eIpVisitFqdnMap.get(i);
                LOG.info("EIpVisitFqdn baseDocumentHashMap大小：" + baseDocumentHashMap.size());
                UpdateEIpVisitFqdn updateEIpVisitFqdn = new UpdateEIpVisitFqdn(baseDocumentHashMap);
                updateEIpVisitFqdn.run();
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    private static String commonSchemaGetFqdn(String commonSchemaType, ResultSet resultSet) {
        String vFqdn = "";
        try {
            switch (commonSchemaType) {
                case "HTTP":
                    vFqdn = resultSet.getString("http_host");
                    break;
                case "SSL":
                    vFqdn = resultSet.getString("ssl_sni");
                    break;
                default:
                    LOG.warn("不支持该类型common_schema_type：" + commonSchemaType);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        if (isDomain(vFqdn)) {
            return vFqdn;
        }
        return "";
    }

    private static boolean isDomain(String fqdn) {
        try {
            String[] fqdnArr = fqdn.split("\\.");
            if (fqdnArr.length < 4 || fqdnArr.length > 4) {
                return true;
            }
            Pattern pattern = Pattern.compile("^[\\d]*$");
            for (String f : fqdnArr) {
                if (pattern.matcher(f).matches()) {
                    int i = Integer.parseInt(f);
                    if (i < 0 || i > 255) {
                        return true;
                    }
                } else {
                    return true;
                }
            }
        } catch (Exception e) {
            LOG.error("解析域名 " + fqdn + " 失败：\n" + e.toString());
        }
        return false;
    }

    private static String getVFqdnSql() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = "common_recv_time >= " + minTime + " AND common_recv_time <= " + maxTime;
        String sslSql = "SELECT ssl_sni AS FQDN,MAX( common_recv_time ) AS LAST_FOUND_TIME,MIN( common_recv_time ) AS FIRST_FOUND_TIME FROM tsg_galaxy_v3.connection_record_log WHERE " + where + " and common_schema_type = 'SSL' GROUP BY ssl_sni";
        String httpSql = "SELECT http_host AS FQDN,MAX( common_recv_time ) AS LAST_FOUND_TIME,MIN( common_recv_time ) AS FIRST_FOUND_TIME FROM tsg_galaxy_v3.connection_record_log WHERE " + where + " and common_schema_type = 'HTTP' GROUP BY http_host";
        return "SELECT FQDN,MAX( LAST_FOUND_TIME ) AS LAST_FOUND_TIME,MIN( FIRST_FOUND_TIME ) AS FIRST_FOUND_TIME FROM ((" + sslSql + ") UNION ALL (" + httpSql + ")) GROUP BY FQDN HAVING FQDN != ''";
    }

    private static String getVIpSql() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = " common_recv_time >= " + minTime + " AND common_recv_time <= " + maxTime + " AND (common_schema_type = 'HTTP' or common_schema_type = 'SSL')";
        String clientIpSql = "SELECT common_client_ip AS IP, common_client_location AS location, common_recv_time FROM tsg_galaxy_v3.connection_record_log where " + where;
        String serverIpSql = "SELECT common_server_ip AS IP, common_server_location AS location, common_recv_time FROM tsg_galaxy_v3.connection_record_log where " + where;
        return "SELECT IP,location,MIN(common_recv_time) AS FIRST_FOUND_TIME,MAX(common_recv_time) AS LAST_FOUND_TIME,COUNT(*) AS IP_COUNT_TOTAL FROM((" + clientIpSql + ") UNION ALL (" + serverIpSql + ")) GROUP BY IP,location";
    }

    private static String getEFqdnAddressIpSql() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = " common_recv_time >= " + minTime + " AND common_recv_time <= " + maxTime;
        String sslSql = "SELECT ssl_sni AS FQDN,common_server_ip,MAX(common_recv_time) AS LAST_FOUND_TIME,MIN(common_recv_time) AS FIRST_FOUND_TIME,COUNT(*) AS COUNT_TOTAL,groupUniqArray(30)(common_client_ip) AS DIST_CIP_RECENT,'SSL' AS  common_schema_type FROM tsg_galaxy_v3.connection_record_log WHERE " + where + " and common_schema_type = 'SSL' GROUP BY ssl_sni,common_server_ip";
        String httpSql = "SELECT http_host AS FQDN,common_server_ip,MAX(common_recv_time) AS LAST_FOUND_TIME,MIN(common_recv_time) AS FIRST_FOUND_TIME,COUNT(*) AS COUNT_TOTAL,groupUniqArray(30)(common_client_ip) AS DIST_CIP_RECENT,'HTTP' AS common_schema_type FROM tsg_galaxy_v3.connection_record_log WHERE " + where + " and common_schema_type = 'HTTP' GROUP BY http_host,common_server_ip";
        return "SELECT * FROM ((" + sslSql + ") UNION ALL (" + httpSql + "))WHERE FQDN != ''";
    }

    private static String getEIpVisitFqdnSql() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = " common_recv_time >= " + minTime + " AND common_recv_time <= " + maxTime;
        String httpSql = "SELECT http_host AS FQDN,common_client_ip,MAX(common_recv_time) AS LAST_FOUND_TIME,MIN(common_recv_time) AS FIRST_FOUND_TIME,COUNT(*) AS COUNT_TOTAL,'HTTP' AS common_schema_type FROM tsg_galaxy_v3.connection_record_log WHERE " + where + " and common_schema_type = 'HTTP' GROUP BY http_host,common_client_ip";
        String sslSql = "SELECT ssl_sni AS FQDN,common_client_ip,MAX(common_recv_time) AS LAST_FOUND_TIME,MIN(common_recv_time) AS FIRST_FOUND_TIME,COUNT(*) AS COUNT_TOTAL,'SSL' AS common_schema_type FROM tsg_galaxy_v3.connection_record_log WHERE common_schema_type = 'SSL' GROUP BY ssl_sni,common_client_ip";
        return "SELECT * FROM ((" + sslSql + ") UNION ALL (" + httpSql + "))WHERE FQDN != ''";
    }

}
