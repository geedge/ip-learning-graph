package cn.ac.iie.service.read;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.utils.TopDomainUtils;
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


    public static final Integer DISTINCT_CLIENT_IP_NUM = 100;
    public static final Integer RECENT_COUNT_HOUR = 24;
    public static final HashSet<String> PROTOCOL_SET;

    static {
        PROTOCOL_SET = new HashSet<>();
        PROTOCOL_SET.add("HTTP");
        PROTOCOL_SET.add("TLS");
        PROTOCOL_SET.add("DNS");
    }

    public static BaseDocument getVertexFqdnDocument(ResultSet resultSet) throws SQLException {
        String fqdnOrReferer = resultSet.getString("FQDN");
        String fqdnName = TopDomainUtils.getDomainFromUrl(fqdnOrReferer);
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
                newDoc.addAttribute("SERVER_SESSION_COUNT", 0L);
                newDoc.addAttribute("SERVER_BYTES_SUM", 0L);
                newDoc.addAttribute("CLIENT_SESSION_COUNT", 0L);
                newDoc.addAttribute("CLIENT_BYTES_SUM", 0L);
                break;
        }
//        newDoc.addAttribute("COMMON_LINK_INFO", "");
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
            newDoc.addAttribute("CNT_TOTAL",countTotal);
            newDoc.addAttribute("DIST_CIP", distCipRecents);
            newDoc.addAttribute("DIST_CIP_TS", clientIpTs);

        }
        return newDoc;
    }

    public static BaseEdgeDocument getRelationshipFqdnSameFqdnDocument(ResultSet resultSet) throws SQLException {
        BaseEdgeDocument newDoc = null;
        String domainFqdn = resultSet.getString("domainFqdn");
        String referer = resultSet.getString("referer");
        String refererFqdn = TopDomainUtils.getDomainFromUrl(referer);
        if (isDomain(refererFqdn) && isDomain(domainFqdn)){
            long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
            long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
            long countTotal = resultSet.getLong("COUNT_TOTAL");
            String key = domainFqdn + "-" + refererFqdn;
            newDoc = new BaseEdgeDocument();
            newDoc.setKey(key);
            newDoc.setFrom("FQDN/" + domainFqdn);
            newDoc.setTo("FQDN/" + refererFqdn);
            newDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime);
            newDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime);
            newDoc.addAttribute("CNT_TOTAL",countTotal);
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

            newDoc = new BaseEdgeDocument();
            newDoc.setKey(key);
            newDoc.setFrom("IP/" + vIp);
            newDoc.setTo("FQDN/" + vFqdn);
            newDoc.addAttribute("CNT_TOTAL",countTotal);
            newDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime);
            newDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime);
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
            if (fqdn == null || fqdn.length() == 0){
                return false;
            }
            String[] fqdnArr = fqdn.split("\\.");
            if (fqdnArr.length < 4 || fqdnArr.length > 4) {
                return true;
            }

            for (String f : fqdnArr) {
                if (pattern.matcher(f).matches()) {
                    long i = Long.parseLong(f);
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
        long[] recentCnt = new long[RECENT_COUNT_HOUR];
        recentCnt[0] = countTotal;
        for (String protocol: PROTOCOL_SET){
            String protocolRecent = protocol +"_CNT_RECENT";
            String protocolTotal  = protocol + "_CNT_TOTAL";
            if (protocol.equals(schema)){
                newDoc.addAttribute(protocolTotal, countTotal);
                newDoc.addAttribute(protocolRecent, recentCnt);
            }else {
                newDoc.addAttribute(protocolTotal, 0L);
                newDoc.addAttribute(protocolRecent, new long[RECENT_COUNT_HOUR]);
            }
        }
    }

    public static String getVertexFqdnSql() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = "recv_time >= "+minTime+" and recv_time <= "+maxTime;
        String mediaDomainSql = "SELECT s1_domain AS FQDN,MIN(recv_time) AS FIRST_FOUND_TIME,MAX(recv_time) AS LAST_FOUND_TIME FROM media_expire_patch WHERE "+where+" and s1_domain != '' GROUP BY s1_domain";
        String refererSql = "SELECT s1_referer AS FQDN,MIN(recv_time) AS FIRST_FOUND_TIME,MAX(recv_time) AS LAST_FOUND_TIME FROM media_expire_patch WHERE "+where+" and s1_referer != '' GROUP BY s1_referer";
        return "SELECT * FROM((" + mediaDomainSql + ") UNION ALL (" + refererSql + "))";
    }

    public static String getVertexIpSql() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = " recv_time >= " + minTime + " AND recv_time < " + maxTime;
        String clientIpSql = "SELECT s1_s_ip AS IP, MIN(recv_time) AS FIRST_FOUND_TIME,MAX(recv_time) AS LAST_FOUND_TIME,count(*) as SESSION_COUNT,sum(media_len) as BYTES_SUM,'client' as ip_type FROM media_expire_patch where " + where + " group by IP";
        String serverIpSql = "SELECT s1_d_ip AS IP, MIN(recv_time) AS FIRST_FOUND_TIME,MAX(recv_time) AS LAST_FOUND_TIME,count(*) as SESSION_COUNT,sum(media_len) as BYTES_SUM,'server' as ip_type FROM media_expire_patch where " + where + " group by IP";
        return "SELECT * FROM((" + clientIpSql + ") UNION ALL (" + serverIpSql + "))";
    }

    public static String getRelationshipFqdnAddressIpSql() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = "recv_time >= "+minTime+" and recv_time <= "+maxTime +" AND s1_domain != '' AND s1_d_ip != '' ";
        return "SELECT s1_domain AS FQDN,s1_d_ip AS common_server_ip,MIN( recv_time ) AS FIRST_FOUND_TIME,MAX( recv_time ) AS LAST_FOUND_TIME,COUNT( * ) AS COUNT_TOTAL,groupUniqArray("+DISTINCT_CLIENT_IP_NUM+")(s1_s_ip) AS DIST_CIP_RECENT FROM media_expire_patch WHERE "+where+" GROUP BY s1_d_ip,s1_domain";
    }

    public static String getRelationshipFqdnSameFqdnSql(){
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = "recv_time >= "+minTime+" and recv_time <= "+maxTime +" AND s1_domain != '' AND s1_referer != '' ";
        return "SELECT s1_domain AS domainFqdn,s1_referer AS referer,MIN(recv_time) AS FIRST_FOUND_TIME,MAX(recv_time) AS LAST_FOUND_TIME,COUNT(*) AS COUNT_TOTAL FROM media_expire_patch where "+where+" GROUP BY s1_domain,s1_referer";
    }

    public static String getRelationshipIpVisitFqdnSql() {
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = "recv_time >= "+minTime+" and recv_time <= "+maxTime+" AND s1_s_ip != '' AND s1_domain != '' ";
        return "SELECT s1_s_ip AS common_client_ip,s1_domain AS FQDN,MIN( recv_time ) AS FIRST_FOUND_TIME,MAX( recv_time ) AS LAST_FOUND_TIME,COUNT( * ) AS COUNT_TOTAL FROM media_expire_patch WHERE "+where+" GROUP BY s1_s_ip,s1_domain";
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
