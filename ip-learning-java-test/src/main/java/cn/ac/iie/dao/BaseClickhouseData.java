package cn.ac.iie.dao;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.etl.UpdateEFqdnAddressIp;
import cn.ac.iie.etl.UpdateEIpVisitFqdn;
import cn.ac.iie.etl.UpdateVFqdn;
import cn.ac.iie.etl.UpdateVIP;
import cn.ac.iie.utils.ClickhouseConnect;
import cn.ac.iie.utils.TopDomainUtils;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

public class BaseClickhouseData {
    private static final ClickhouseConnect manger = ClickhouseConnect.getInstance();
    private static HashMap<Integer, HashMap<String, ArrayList<BaseDocument>>> vFqdnMap =  new HashMap<>();
    private static HashMap<Integer, HashMap<String,BaseDocument>> vIpMap =  new HashMap<>();
    private static HashMap<Integer, HashMap<String,BaseEdgeDocument>> eFqdnAddressIpMap =  new HashMap<>();
    private static HashMap<Integer, HashMap<String,BaseEdgeDocument>> eIpVisitFqdnMap =  new HashMap<>();
    public Connection connection;
    public Statement pstm;

    public BaseClickhouseData(){}

    private static long[] getTimeLimit(){
        long maxTime = ApplicationConfig.READ_CLICKHOUSE_MAX_TIME;
        long minTime = ApplicationConfig.READ_CLICKHOUSE_MIN_TIME;
        return new long[]{maxTime,minTime};
    }

    static {
        for (int i = 0;i < ApplicationConfig.THREAD_POOL_NUMBER;i++){
            vFqdnMap.put(i,new HashMap<String, ArrayList<BaseDocument>>());
        }
        System.out.println("V_FQDN resultMap初始化完成");
        for (int i = 0;i < ApplicationConfig.THREAD_POOL_NUMBER;i++){
            vIpMap.put(i,new HashMap<String, BaseDocument>());
        }
        System.out.println("V_IP resultMap初始化完成");
        for (int i = 0;i < ApplicationConfig.THREAD_POOL_NUMBER;i++){
            eFqdnAddressIpMap.put(i,new HashMap<String, BaseEdgeDocument>());
        }
        System.out.println("E_ADDRESS_V_FQDN_TO_V_IP resultMap初始化完成");
        for (int i = 0;i < ApplicationConfig.THREAD_POOL_NUMBER;i++){
            eIpVisitFqdnMap.put(i,new HashMap<String, BaseEdgeDocument>());
        }
        System.out.println("E_VISIT_V_IP_TO_V_FQDN resultMap初始化完成");
    }

    public static void BaseVFqdn(){
        BaseVDomainFromReferer();
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = "recv_time >= "+minTime+" and recv_time <= "+maxTime+" and media_domain != '' ";
        String sql = "SELECT media_domain AS FQDN_NAME,MIN(recv_time) AS FIRST_FOUND_TIME,MAX(recv_time) AS LAST_FOUND_TIME,COUNT( * ) AS FQDN_COUNT_TOTAL FROM media_expire_patch WHERE "+where+" GROUP BY media_domain";
        System.out.println(sql);
        long start = System.currentTimeMillis();
        try {
            DruidPooledConnection connection = manger.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()){
                String fqdnName = resultSet.getString("FQDN_NAME");
                long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                long fqdnCountTotal = resultSet.getLong("FQDN_COUNT_TOTAL");
                BaseDocument newDoc = new BaseDocument();
                newDoc.setKey(fqdnName);
                newDoc.addAttribute("FQDN_NAME",fqdnName);
                newDoc.addAttribute("FIRST_FOUND_TIME",firstFoundTime);
                newDoc.addAttribute("LAST_FOUND_TIME",lastFoundTime);
                newDoc.addAttribute("FQDN_COUNT_TOTAL",fqdnCountTotal);
                int i = fqdnName.hashCode() % ApplicationConfig.THREAD_POOL_NUMBER;
                HashMap<String, ArrayList<BaseDocument>> documentHashMap = vFqdnMap.getOrDefault(i, new HashMap<>());
                ArrayList<BaseDocument> documentArrayList = documentHashMap.getOrDefault(fqdnName, new ArrayList<>());
                documentArrayList.add(newDoc);
                documentHashMap.put(fqdnName,documentArrayList);
            }
            long last = System.currentTimeMillis();
            System.out.println("读取clickhouse v_FQDN时间："+(last - start));
            for (int i = 0;i < ApplicationConfig.THREAD_POOL_NUMBER;i++){
                HashMap<String,  ArrayList<BaseDocument>> baseDocumentHashMap = vFqdnMap.get(i);
                UpdateVFqdn updateVFqdn = new UpdateVFqdn(baseDocumentHashMap);
                updateVFqdn.run();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static void BaseVDomainFromReferer(){
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = "recv_time >= "+minTime+" and recv_time <= "+maxTime+" and s1_referer != '' ";
        String sql = "SELECT s1_referer AS FQDN_NAME,MIN(recv_time) AS FIRST_FOUND_TIME,MAX(recv_time) AS LAST_FOUND_TIME,COUNT( * ) AS FQDN_COUNT_TOTAL FROM media_expire_patch WHERE "+where+" GROUP BY s1_referer";
        System.out.println(sql);
        long start = System.currentTimeMillis();
        try {
            DruidPooledConnection connection = manger.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()){
                String referer = resultSet.getString("FQDN_NAME");
                String fqdnName = TopDomainUtils.getDomainFromUrl(referer);
                long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                long fqdnCountTotal = resultSet.getLong("FQDN_COUNT_TOTAL");
                BaseDocument newDoc = new BaseDocument();
                newDoc.setKey(fqdnName);
                newDoc.addAttribute("FQDN_NAME",fqdnName);
                newDoc.addAttribute("FIRST_FOUND_TIME",firstFoundTime);
                newDoc.addAttribute("LAST_FOUND_TIME",lastFoundTime);
                newDoc.addAttribute("FQDN_COUNT_TOTAL",fqdnCountTotal);
                int i = fqdnName.hashCode() % ApplicationConfig.THREAD_POOL_NUMBER;
                HashMap<String, ArrayList<BaseDocument>> documentHashMap = vFqdnMap.getOrDefault(i, new HashMap<>());
                ArrayList<BaseDocument> documentArrayList = documentHashMap.getOrDefault(fqdnName, new ArrayList<>());
                documentArrayList.add(newDoc);
                documentHashMap.put(fqdnName,documentArrayList);
            }
            long last = System.currentTimeMillis();
            System.out.println("读取clickhouse v_FQDN时间："+(last - start));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void BaseVIp(){
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = " recv_time >= "+minTime+" and recv_time <= "+maxTime;
        String sql = "SELECT IP,location,MIN(recv_time) AS FIRST_FOUND_TIME,MAX(recv_time) AS LAST_FOUND_TIME,COUNT(*) AS IP_COUNT_TOTAL FROM(( SELECT s1_s_ip AS IP, s1_s_location_region AS location, recv_time FROM media_expire_patch where "+where+" ) UNION ALL ( SELECT s1_d_ip AS IP, s1_d_location_region AS location, recv_time FROM media_expire_patch where "+where+" )) GROUP BY IP,location";
        System.out.println(sql);
        long start = System.currentTimeMillis();
        try {
            DruidPooledConnection connection = manger.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()){
                String ip = resultSet.getString("IP");
                String location = resultSet.getString("location");
                long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                long ipCountTotal = resultSet.getLong("IP_COUNT_TOTAL");
                BaseDocument newDoc = new BaseDocument();
                newDoc.setKey(ip);
                newDoc.addAttribute("IP",ip);
                newDoc.addAttribute("IP_LOCATION",location);
                newDoc.addAttribute("FIRST_FOUND_TIME",firstFoundTime);
                newDoc.addAttribute("LAST_FOUND_TIME",lastFoundTime);
                newDoc.addAttribute("IP_COUNT_TOTAL",ipCountTotal);
                int i = ip.hashCode() % ApplicationConfig.THREAD_POOL_NUMBER;
                HashMap<String, BaseDocument> documentHashMap = vIpMap.getOrDefault(i, new HashMap<String, BaseDocument>());
                documentHashMap.put(ip,newDoc);
            }
            long last = System.currentTimeMillis();
            System.out.println("读取clickhouse v_IP时间："+(last - start));
            for (int i = 0;i < ApplicationConfig.THREAD_POOL_NUMBER;i++){
                HashMap<String, BaseDocument> baseDocumentHashMap = vIpMap.get(i);
                UpdateVIP updateVIp = new UpdateVIP(baseDocumentHashMap);
                updateVIp.run();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void BaseEFqdnAddressIp(){
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = "recv_time >= "+minTime+" and recv_time <= "+maxTime +" AND media_domain != '' AND s1_d_ip != '' ";
        String sql = "SELECT media_domain AS V_FQDN,s1_d_ip AS V_IP,MIN( recv_time ) AS FIRST_FOUND_TIME,MAX( recv_time ) AS LAST_FOUND_TIME,COUNT( * ) AS COUNT_TOTAL FROM media_expire_patch WHERE "+where+" GROUP BY s1_d_ip,media_domain";
        System.out.println(sql);
        long start = System.currentTimeMillis();
        try {
            DruidPooledConnection connection = manger.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()){
                String vFqdn = resultSet.getString("V_FQDN");
                String vIp = resultSet.getString("V_IP");
                long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                long countTotal = resultSet.getLong("COUNT_TOTAL");
                String key = vFqdn+"-"+vIp;
                BaseEdgeDocument newDoc = new BaseEdgeDocument();
                newDoc.setKey(key);
                newDoc.setFrom("V_FQDN/"+vFqdn);
                newDoc.setTo("V_IP/"+vIp);
                newDoc.addAttribute("FIRST_FOUND_TIME",firstFoundTime);
                newDoc.addAttribute("LAST_FOUND_TIME",lastFoundTime);
                newDoc.addAttribute("COUNT_TOTAL",countTotal);
                int i = key.hashCode() % ApplicationConfig.THREAD_POOL_NUMBER;
                HashMap<String, BaseEdgeDocument> documentHashMap = eFqdnAddressIpMap.getOrDefault(i, new HashMap<String, BaseEdgeDocument>());
                documentHashMap.put(key,newDoc);
            }
            long last = System.currentTimeMillis();
            System.out.println("读取clickhouse EFqdnAddressIp时间："+(last - start));
            for (int i = 0;i < ApplicationConfig.THREAD_POOL_NUMBER;i++){
                HashMap<String, BaseEdgeDocument> baseDocumentHashMap = eFqdnAddressIpMap.get(i);
                UpdateEFqdnAddressIp updateEFqdnAddressIp = new UpdateEFqdnAddressIp(baseDocumentHashMap);
                updateEFqdnAddressIp.run();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void BaseEdgeFqdnSameFqdn(){
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = "recv_time >= "+minTime+" and recv_time <= "+maxTime +" AND media_domain != '' AND s1_d_ip != '' ";
        String sql = "SELECT s1_domain AS V_FQDN,s1_referer,MIN(recv_time) AS FIRST_FOUND_TIME,MAX( recv_time ) AS LAST_FOUND_TIME,COUNT( * ) AS COUNT_TOTAL FROM media_expire_patch WHERE "+where+" GROUP BY s1_referer,s1_domain";
        System.out.println(sql);
        try {
            DruidPooledConnection connection = manger.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()){
                String vFqdn = resultSet.getString("V_FQDN");
                String referer = resultSet.getString("s1_referer");
                String refererDomain = TopDomainUtils.getDomainFromUrl(referer);
                long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                long countTotal = resultSet.getLong("COUNT_TOTAL");
                String key = vFqdn+"-"+refererDomain;

            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void BaseEIpVisitFqdn(){
        long[] timeLimit = getTimeLimit();
        long maxTime = timeLimit[0];
        long minTime = timeLimit[1];
        String where = "recv_time >= "+minTime+" and recv_time <= "+maxTime+" AND s1_s_ip != '' AND media_domain != '' ";
        String sql = "SELECT s1_s_ip AS V_IP,media_domain AS V_FQDN,MIN( recv_time ) AS FIRST_FOUND_TIME,MAX( recv_time ) AS LAST_FOUND_TIME,COUNT( * ) AS COUNT_TOTAL FROM media_expire_patch WHERE "+where+" GROUP BY s1_s_ip,media_domain";
        System.out.println(sql);
        long start = System.currentTimeMillis();
        try {
            DruidPooledConnection connection = manger.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()){
                String vIp = resultSet.getString("V_IP");
                String vFqdn = resultSet.getString("V_FQDN");
                long firstFoundTime = resultSet.getLong("FIRST_FOUND_TIME");
                long lastFoundTime = resultSet.getLong("LAST_FOUND_TIME");
                long countTotal = resultSet.getLong("COUNT_TOTAL");
                String key = vIp +"-"+ vFqdn;
                BaseEdgeDocument newDoc = new BaseEdgeDocument();
                newDoc.setKey(key);
                newDoc.setFrom("V_IP/"+vIp);
                newDoc.setTo("V_FQDN/"+vFqdn);
                newDoc.addAttribute("FIRST_FOUND_TIME",firstFoundTime);
                newDoc.addAttribute("LAST_FOUND_TIME",lastFoundTime);
                newDoc.addAttribute("COUNT_TOTAL",countTotal);
                int i = key.hashCode() % ApplicationConfig.THREAD_POOL_NUMBER;
                HashMap<String, BaseEdgeDocument> documentHashMap = eIpVisitFqdnMap.getOrDefault(i, new HashMap<String, BaseEdgeDocument>());
                documentHashMap.put(key,newDoc);
            }
            long last = System.currentTimeMillis();
            System.out.println("读取clickhouse EIpVisitFqdn时间："+(last - start));
            for (int i = 0;i < ApplicationConfig.THREAD_POOL_NUMBER;i++){
                HashMap<String, BaseEdgeDocument> baseDocumentHashMap = eIpVisitFqdnMap.get(i);
                UpdateEIpVisitFqdn updateEIpVisitFqdn = new UpdateEIpVisitFqdn(baseDocumentHashMap);
                updateEIpVisitFqdn.run();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
