package cn.ac.iie.service.read;

import cn.ac.iie.config.ApplicationConfig;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

/**
 * @author wlh
 */
public class ReadClickhouseData {

    public static long currentHour = System.currentTimeMillis() / (60 * 60 * 1000) * 60 * 60;

    private static Pattern pattern = Pattern.compile("^[\\d]*$");
    private static final Logger LOG = LoggerFactory.getLogger(ReadClickhouseData.class);


    public static HashSet<String> protocolSet;

    static {
        protocolSet = new HashSet<>();
        protocolSet.add("HTTP");
        protocolSet.add("TLS");
        protocolSet.add("DNS");
    }

    public static BaseDocument getVertexFqdnDocument(ResultSet resultSet) throws SQLException {
        String fqdnName = resultSet.getString("FQDN");
        BaseDocument newDoc = null;
        if (isDomain(fqdnName)) {
            long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
            long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
            newDoc = new BaseDocument();
            newDoc.setKey(fqdnName);
            newDoc.addAttribute("FQDN_NAME", fqdnName);
            newDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime);
            newDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime);
        }
        return newDoc;
    }

    public static BaseDocument getVertexIpDocument(ResultSet resultSet) throws SQLException {
        BaseDocument newDoc = new BaseDocument();
        String ip = resultSet.getString("IP");
        long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
        long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
        long sessionCount = resultSet.getLong("SESSION_COUNT");
        long bytesSum = resultSet.getLong("BYTES_SUM");
        String ipType = resultSet.getString("ip_type");
        newDoc.setKey(ip);
        newDoc.addAttribute("IP", ip);
        newDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime);
        newDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime);
        switch (ipType) {
            case "client":
                newDoc.addAttribute("CLIENT_SESSION_COUNT", sessionCount);
                newDoc.addAttribute("CLIENT_BYTES_SUM", bytesSum);
                newDoc.addAttribute("SERVER_SESSION_COUNT", 0L);
                newDoc.addAttribute("SERVER_BYTES_SUM", 0L);
                break;
            case "server":
                newDoc.addAttribute("SERVER_SESSION_COUNT", sessionCount);
                newDoc.addAttribute("SERVER_BYTES_SUM", bytesSum);
                newDoc.addAttribute("CLIENT_SESSION_COUNT", 0L);
                newDoc.addAttribute("CLIENT_BYTES_SUM", 0L);
                break;
            default:
        }
        newDoc.addAttribute("COMMON_LINK_INFO", "");
        return newDoc;
    }

    public static BaseDocument getVertexSubscriberDocument(ResultSet resultSet) throws SQLException {
        String subscriberId = resultSet.getString("common_subscriber_id");
        long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
        long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
        BaseDocument newDoc = new BaseDocument();
        newDoc.setKey(subscriberId);
        newDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime);
        newDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime);
        return newDoc;
    }

    public static BaseEdgeDocument getRelationshipSubsciberLocateIpDocument(ResultSet resultSet) throws SQLException {
        String subscriberId = resultSet.getString("common_subscriber_id");
        String framedIp = resultSet.getString("radius_framed_ip");
        long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
        long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
        long countTotal = resultSet.getLong("COUNT_TOTAL");

        String key = subscriberId + "-" + framedIp;
        BaseEdgeDocument newDoc = new BaseEdgeDocument();
        newDoc.setKey(key);
        newDoc.setFrom("SUBSCRIBER/" + subscriberId);
        newDoc.setTo("IP/" + framedIp);
        newDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime);
        newDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime);
        newDoc.addAttribute("COUNT_TOTAL", countTotal);

        return newDoc;

    }

    public static BaseEdgeDocument getRelationFqdnAddressIpDocument(ResultSet resultSet) throws SQLException {
        String vFqdn = resultSet.getString("FQDN");
        BaseEdgeDocument newDoc = null;
        if (isDomain(vFqdn)) {
            String vIp = resultSet.getString("common_server_ip");
            long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
            long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
            long countTotal = resultSet.getLong("COUNT_TOTAL");
            String schemaType = resultSet.getString("schema_type");
            String[] distCipRecents = (String[]) resultSet.getArray("DIST_CIP_RECENT").getArray();
            long[] clientIpTs = new long[distCipRecents.length];
            for (int i = 0; i < clientIpTs.length; i++) {
                clientIpTs[i] = currentHour;
            }

            String key = vFqdn + "-" + vIp;
            newDoc = new BaseEdgeDocument();
            newDoc.setKey(key);
            newDoc.setFrom("FQDN/" + vFqdn);
            newDoc.setTo("IP/" + vIp);
            newDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime);
            newDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime);
            newDoc.addAttribute("DIST_CIP", distCipRecents);
            newDoc.addAttribute("DIST_CIP_TS", clientIpTs);

            initSchemaProperty(newDoc);

            if (protocolSet.contains(schemaType)){
                checkSchemaProperty(newDoc, schemaType, countTotal);
            }
        }
        return newDoc;
    }

    public static BaseEdgeDocument getRelationIpVisitFqdnDocument(ResultSet resultSet) throws SQLException {
        BaseEdgeDocument newDoc = null;
        String vFqdn = resultSet.getString("FQDN");
        if (isDomain(vFqdn)) {
            String vIp = resultSet.getString("common_client_ip");
            String key = vIp + "-" + vFqdn;
            long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
            long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
            long countTotal = resultSet.getLong("COUNT_TOTAL");
            String schemaType = resultSet.getString("schema_type");

            newDoc = new BaseEdgeDocument();
            newDoc.setKey(key);
            newDoc.setFrom("IP/" + vIp);
            newDoc.setTo("FQDN/" + vFqdn);
            newDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime);
            newDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime);

            initSchemaProperty(newDoc);

            if (protocolSet.contains(schemaType)){
                checkSchemaProperty(newDoc, schemaType, countTotal);
            }
        }
        return newDoc;
    }

    public static <T extends BaseDocument> void putMapByHashcode(T newDoc, HashMap<Integer, HashMap<String, ArrayList<T>>> map) {
        if (newDoc != null) {
            String key = newDoc.getKey();
            int i = Math.abs(key.hashCode()) % ApplicationConfig.THREAD_POOL_NUMBER;
            HashMap<String, ArrayList<T>> documentHashMap = map.getOrDefault(i, new HashMap<>());
            ArrayList<T> documentArrayList = documentHashMap.getOrDefault(key, new ArrayList<>());
            documentArrayList.add(newDoc);
            documentHashMap.put(key, documentArrayList);
        }
    }

    private static boolean isDomain(String fqdn) {
        try {
            String[] fqdnArr = fqdn.split("\\.");
            if (fqdnArr.length < 4 || fqdnArr.length > 4) {
                return true;
            }

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


    private static void checkSchemaProperty(BaseEdgeDocument newDoc, String schema, long countTotal) {
        long[] recentCnt = new long[24];
        recentCnt[0] = countTotal;
        String protocolRecent = schema +"_CNT_RECENT";
        String protocolTotal = schema + "_CNT_TOTAL";
        newDoc.updateAttribute(protocolTotal, countTotal);
        newDoc.updateAttribute(protocolRecent, recentCnt);
        newDoc.addAttribute("PROTOCOL_TYPE", schema);
    }

    private static void initSchemaProperty(BaseEdgeDocument newDoc){
        newDoc.addAttribute("HTTP_CNT_TOTAL", 0L);
        newDoc.addAttribute("HTTP_CNT_RECENT", new long[24]);
        newDoc.addAttribute("TLS_CNT_TOTAL", 0L);
        newDoc.addAttribute("TLS_CNT_RECENT", new long[24]);
        newDoc.addAttribute("DNS_CNT_TOTAL", 0L);
        newDoc.addAttribute("DNS_CNT_RECENT", new long[24]);
    }

    public static String getVertexFqdnSql() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = "common_recv_time >= " + minTime + " AND common_recv_time < " + maxTime;
        String sslSql = "SELECT ssl_sni AS FQDN,MAX( common_recv_time ) AS LAST_FOUND_TIME,MIN( common_recv_time ) AS FIRST_FOUND_TIME FROM tsg_galaxy_v3.connection_record_log WHERE " + where + " and common_schema_type = 'SSL' GROUP BY ssl_sni";
        String httpSql = "SELECT http_host AS FQDN,MAX( common_recv_time ) AS LAST_FOUND_TIME,MIN( common_recv_time ) AS FIRST_FOUND_TIME FROM tsg_galaxy_v3.connection_record_log WHERE " + where + " and common_schema_type = 'HTTP' GROUP BY http_host";
        return "SELECT FQDN,MAX( LAST_FOUND_TIME ) AS LAST_FOUND_TIME,MIN( FIRST_FOUND_TIME ) AS FIRST_FOUND_TIME FROM ((" + sslSql + ") UNION ALL (" + httpSql + ")) GROUP BY FQDN HAVING FQDN != ''";
    }

    public static String getVertexIpSql() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = " common_recv_time >= " + minTime + " AND common_recv_time < " + maxTime;
        String clientIpSql = "SELECT common_client_ip AS IP, MIN(common_recv_time) AS FIRST_FOUND_TIME,MAX(common_recv_time) AS LAST_FOUND_TIME,count(*) as SESSION_COUNT,sum(common_c2s_byte_num) as BYTES_SUM,'client' as ip_type FROM tsg_galaxy_v3.connection_record_log where " + where + " group by IP";
        String serverIpSql = "SELECT common_server_ip AS IP, MIN(common_recv_time) AS FIRST_FOUND_TIME,MAX(common_recv_time) AS LAST_FOUND_TIME,count(*) as SESSION_COUNT,sum(common_s2c_byte_num) as BYTES_SUM,'server' as ip_type FROM tsg_galaxy_v3.connection_record_log where " + where + " group by IP";
        return "SELECT * FROM((" + clientIpSql + ") UNION ALL (" + serverIpSql + "))";
    }

    public static String getRelationshipFqdnAddressIpSql() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = " common_recv_time >= " + minTime + " AND common_recv_time < " + maxTime;
        String sslSql = "SELECT ssl_sni AS FQDN,common_server_ip,MAX(common_recv_time) AS LAST_FOUND_TIME,MIN(common_recv_time) AS FIRST_FOUND_TIME,COUNT(*) AS COUNT_TOTAL,groupUniqArray(100)(common_client_ip) AS DIST_CIP_RECENT,'TLS' AS  schema_type FROM tsg_galaxy_v3.connection_record_log WHERE " + where + " and common_schema_type = 'SSL' GROUP BY ssl_sni,common_server_ip";
        String httpSql = "SELECT http_host AS FQDN,common_server_ip,MAX(common_recv_time) AS LAST_FOUND_TIME,MIN(common_recv_time) AS FIRST_FOUND_TIME,COUNT(*) AS COUNT_TOTAL,groupUniqArray(100)(common_client_ip) AS DIST_CIP_RECENT,'HTTP' AS schema_type FROM tsg_galaxy_v3.connection_record_log WHERE " + where + " and common_schema_type = 'HTTP' GROUP BY http_host,common_server_ip";
        return "SELECT * FROM ((" + sslSql + ") UNION ALL (" + httpSql + "))WHERE FQDN != ''";
    }

    public static String getRelationshipIpVisitFqdnSql() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = " common_recv_time >= " + minTime + " AND common_recv_time < " + maxTime;
        String httpSql = "SELECT http_host AS FQDN,common_client_ip,MAX(common_recv_time) AS LAST_FOUND_TIME,MIN(common_recv_time) AS FIRST_FOUND_TIME,COUNT(*) AS COUNT_TOTAL,'HTTP' AS schema_type FROM tsg_galaxy_v3.connection_record_log WHERE " + where + " and common_schema_type = 'HTTP' GROUP BY http_host,common_client_ip";
        String sslSql = "SELECT ssl_sni AS FQDN,common_client_ip,MAX(common_recv_time) AS LAST_FOUND_TIME,MIN(common_recv_time) AS FIRST_FOUND_TIME,COUNT(*) AS COUNT_TOTAL,'TLS' AS schema_type FROM tsg_galaxy_v3.connection_record_log WHERE common_schema_type = 'SSL' GROUP BY ssl_sni,common_client_ip";
        return "SELECT * FROM ((" + sslSql + ") UNION ALL (" + httpSql + "))WHERE FQDN != ''";
    }

    public static String getVertexSubscriberSql() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = " common_recv_time >= " + minTime + " AND common_recv_time < " + maxTime + " AND common_subscriber_id != '' AND radius_packet_type = 4 AND radius_acct_status_type = 1";
        return "SELECT common_subscriber_id,MAX(common_recv_time) as LAST_FOUND_TIME,MIN(common_recv_time) as FIRST_FOUND_TIME FROM radius_record_log WHERE" + where + " GROUP BY common_subscriber_id";
    }

    public static String getRelationshipSubsciberLocateIpSql() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = " common_recv_time >= " + minTime + " AND common_recv_time < " + maxTime + " AND common_subscriber_id != '' AND radius_framed_ip != '' AND radius_packet_type = 4 AND radius_acct_status_type = 1";
        return "SELECT common_subscriber_id,radius_framed_ip,MAX(common_recv_time) as LAST_FOUND_TIME,MIN(common_recv_time) as FIRST_FOUND_TIME,COUNT(*) as COUNT_TOTAL FROM radius_record_log WHERE" + where + " GROUP BY common_subscriber_id,radius_framed_ip";
    }

    private static long[] getTimeLimit() {
//        long maxTime = currentHour;
//        long minTime = maxTime - 3600;
        long maxTime = ApplicationConfig.READ_CLICKHOUSE_MAX_TIME;
        long minTime = ApplicationConfig.READ_CLICKHOUSE_MIN_TIME;
        return new long[]{maxTime, minTime};
    }

}
