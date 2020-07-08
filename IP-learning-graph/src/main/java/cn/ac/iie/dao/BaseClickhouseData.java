package cn.ac.iie.dao;

import cn.ac.iie.config.ApplicationConfig;
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
import java.util.Map;

import static cn.ac.iie.etl.read.ReadClickhouseData.*;

public class BaseClickhouseData {
    private static final Logger LOG = LoggerFactory.getLogger(BaseClickhouseData.class);

    private static final ClickhouseConnect manger = ClickhouseConnect.getInstance();
    static HashMap<Integer, HashMap<String, ArrayList<BaseDocument>>> vFqdnMap = new HashMap<>();
    static HashMap<Integer, HashMap<String, ArrayList<BaseDocument>>> vIpMap = new HashMap<>();
    static HashMap<Integer, HashMap<String, HashMap<String, BaseEdgeDocument>>> eFqdnAddressIpMap = new HashMap<>();
    static HashMap<Integer, HashMap<String, HashMap<String, BaseEdgeDocument>>> eIpVisitFqdnMap = new HashMap<>();

    private DruidPooledConnection connection;
    private Statement statement;

    public void BaseVFqdn() {
        initializeVertexMap(vFqdnMap);
        LOG.info("V_FQDN resultMap初始化完成");
        String sql = getVFqdnSql();
        long start = System.currentTimeMillis();
        try {
            connection = manger.getConnection();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                BaseDocument newDoc = getVertexFqdnDocument(resultSet);
                if (newDoc != null) {
                    String fqdnName = newDoc.getKey();
                    int i = Math.abs(fqdnName.hashCode()) % ApplicationConfig.THREAD_POOL_NUMBER;
                    HashMap<String, ArrayList<BaseDocument>> documentHashMap = vFqdnMap.getOrDefault(i, new HashMap<>());
                    ArrayList<BaseDocument> documentArrayList = documentHashMap.getOrDefault(fqdnName, new ArrayList<>());
                    documentArrayList.add(newDoc);
                    documentHashMap.put(fqdnName,documentArrayList);
                }
            }
            long last = System.currentTimeMillis();
            LOG.info(sql + "\n读取clickhouse v_FQDN时间：" + (last - start));
        } catch (Exception e) {
            LOG.error(e.toString());
        }finally {
            manger.clear(statement,connection);
        }
    }

    public void BaseVIp() {
        initializeVertexMap(vIpMap);
        LOG.info("V_IP resultMap初始化完成");
        String sql = getVIpSql();
        long start = System.currentTimeMillis();
        try {
            connection = manger.getConnection();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                BaseDocument newDoc = getVertexIpDocument(resultSet);
                String ip = newDoc.getKey();
                int i = Math.abs(ip.hashCode()) % ApplicationConfig.THREAD_POOL_NUMBER;
                HashMap<String, ArrayList<BaseDocument>> documentHashMap = vIpMap.getOrDefault(i, new HashMap<>());
                ArrayList<BaseDocument> documentArrayList = documentHashMap.getOrDefault(ip, new ArrayList<>());
                documentArrayList.add(newDoc);
                documentHashMap.put(ip,documentArrayList);
            }
            long last = System.currentTimeMillis();
            LOG.info(sql + "\n读取clickhouse v_IP时间：" + (last - start));
        } catch (Exception e) {
            LOG.error(e.toString());
        }finally {
            manger.clear(statement,connection);
        }
    }

    public void BaseEFqdnAddressIp() {
        initializeVertexMap(eFqdnAddressIpMap);
        LOG.info("E_ADDRESS_V_FQDN_TO_V_IP resultMap初始化完成");
        String sql = getEFqdnAddressIpSql();
        long start = System.currentTimeMillis();
        try {
            connection = manger.getConnection();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                BaseEdgeDocument newDoc = getRelationFqdnAddressIpDocument(resultSet);
                putMapByHashcode(resultSet, newDoc, eFqdnAddressIpMap);
            }
            long last = System.currentTimeMillis();
            LOG.info(sql + "\n读取clickhouse EFqdnAddressIp时间：" + (last - start));
        } catch (Exception e) {
            LOG.error(e.toString());
        }finally {
            manger.clear(statement,connection);
        }
    }

    public void BaseEIpVisitFqdn() {
        initializeVertexMap(eIpVisitFqdnMap);
        LOG.info("E_VISIT_V_IP_TO_V_FQDN resultMap初始化完成");
        String sql = getEIpVisitFqdnSql();
        long start = System.currentTimeMillis();
        try {
            connection = manger.getConnection();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                BaseEdgeDocument newDoc = getRelationIpVisitFqdnDocument(resultSet);
                putMapByHashcode(resultSet, newDoc, eIpVisitFqdnMap);
            }
            long last = System.currentTimeMillis();
            LOG.info(sql + "\n读取clickhouse EIpVisitFqdn时间：" + (last - start));
        } catch (Exception e) {
            LOG.error(e.toString());
        }finally {
            manger.clear(statement,connection);
        }
    }

    private void initializeVertexMap(Map map){
        try {
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                map.put(i, new HashMap<>());
            }
        }catch (Exception e){
            e.printStackTrace();
            LOG.error("初始化数据失败");
        }
    }


}
