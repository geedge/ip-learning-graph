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

import static cn.ac.iie.service.read.ReadClickhouseData.*;

/**
 * 读取clickhouse数据，封装到map
 * @author wlh
 */
public class BaseClickhouseData {
    private static final Logger LOG = LoggerFactory.getLogger(BaseClickhouseData.class);

    private static ClickhouseConnect manger = ClickhouseConnect.getInstance();
    static HashMap<Integer, HashMap<String, ArrayList<BaseDocument>>> vFqdnMap = new HashMap<>();
    static HashMap<Integer, HashMap<String, ArrayList<BaseDocument>>> vIpMap = new HashMap<>();
    static HashMap<Integer, HashMap<String,ArrayList<BaseDocument>>> vSubscriberMap = new HashMap<>();
    static HashMap<Integer, HashMap<String, ArrayList<BaseEdgeDocument>>> eFqdnAddressIpMap = new HashMap<>();
    static HashMap<Integer, HashMap<String, ArrayList<BaseEdgeDocument>>> eIpVisitFqdnMap = new HashMap<>();
    static HashMap<Integer, HashMap<String, ArrayList<BaseEdgeDocument>>> eSubsciberLocateIpMap = new HashMap<>();

    private DruidPooledConnection connection;
    private Statement statement;

    void baseVertexFqdn() {
        initializeMap(vFqdnMap);
        LOG.info("FQDN resultMap初始化完成");
        String sql = getVertexFqdnSql();
        long start = System.currentTimeMillis();
        try {
            connection = manger.getConnection();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                BaseDocument newDoc = getVertexFqdnDocument(resultSet);
                if (newDoc != null) {
                    putMapByHashcode(newDoc,vFqdnMap);
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

    void baseVertexIp() {
        initializeMap(vIpMap);
        LOG.info("IP resultMap初始化完成");
        String sql = getVertexIpSql();
        long start = System.currentTimeMillis();
        try {
            connection = manger.getConnection();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                BaseDocument newDoc = getVertexIpDocument(resultSet);
                putMapByHashcode(newDoc,vIpMap);
            }
            long last = System.currentTimeMillis();
            LOG.info(sql + "\n读取clickhouse v_IP时间：" + (last - start));
        } catch (Exception e) {
            LOG.error(e.toString());
        }finally {
            manger.clear(statement,connection);
        }
    }

    void baseVertexSubscriber(){
        initializeMap(vSubscriberMap);
        LOG.info("SUBSCRIBER resultMap初始化完成");
        String sql = getVertexSubscriberSql();
        long start = System.currentTimeMillis();
        try {
            connection = manger.getConnection();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()){
                BaseDocument newDoc = getVertexSubscriberDocument(resultSet);
                putMapByHashcode(newDoc,vSubscriberMap);
            }
            long last = System.currentTimeMillis();
            LOG.info(sql + "\n读取clickhouse v_SUBSCRIBER时间：" + (last - start));
        }catch (Exception e){
            LOG.error(sql + "\n读取clickhouse v_SUBSCRIBER失败");
            e.printStackTrace();
        }finally {
            manger.clear(statement,connection);
        }
    }

    void baseRelationshipSubscriberLocateIp(){
        initializeMap(eSubsciberLocateIpMap);
        LOG.info("R_LOCATE_SUBSCRIBER2IP");
        String sql = getRelationshipSubsciberLocateIpSql();
        long start = System.currentTimeMillis();
        try {
            connection = manger.getConnection();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()){
                BaseEdgeDocument newDoc = getRelationshipSubsciberLocateIpDocument(resultSet);
                putMapByHashcode(newDoc,eSubsciberLocateIpMap);
            }
            long last = System.currentTimeMillis();
            LOG.info(sql + "\n读取clickhouse ESubsciberLocateIp时间：" + (last - start));
        }catch (Exception e){
            LOG.error(sql + "\n读取clickhouse ESubsciberLocateIp失败");
            e.printStackTrace();
        }finally {
            manger.clear(statement,connection);
        }
    }

    void baseRelationshipFqdnAddressIp() {
        initializeMap(eFqdnAddressIpMap);
        LOG.info("R_LOCATE_FQDN2IP resultMap初始化完成");
        String sql = getRelationshipFqdnAddressIpSql();
        long start = System.currentTimeMillis();
        try {
            connection = manger.getConnection();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                BaseEdgeDocument newDoc = getRelationFqdnAddressIpDocument(resultSet);
                putMapByHashcode(newDoc,eFqdnAddressIpMap);
            }
            long last = System.currentTimeMillis();
            LOG.info(sql + "\n读取clickhouse EFqdnAddressIp时间：" + (last - start));
        } catch (Exception e) {
            LOG.error(e.toString());
        }finally {
            manger.clear(statement,connection);
        }
    }

    void baseRelationshipIpVisitFqdn() {
        initializeMap(eIpVisitFqdnMap);
        LOG.info("R_VISIT_IP2FQDN resultMap初始化完成");
        String sql = getRelationshipIpVisitFqdnSql();
        long start = System.currentTimeMillis();
        try {
            connection = manger.getConnection();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                BaseEdgeDocument newDoc = getRelationIpVisitFqdnDocument(resultSet);
                putMapByHashcode(newDoc,eIpVisitFqdnMap);
            }
            long last = System.currentTimeMillis();
            LOG.info(sql + "\n读取clickhouse EIpVisitFqdn时间：" + (last - start));
        } catch (Exception e) {
            LOG.error(e.toString());
        }finally {
            manger.clear(statement,connection);
        }
    }

    private <T extends BaseDocument> void initializeMap(HashMap<Integer, HashMap<String,ArrayList<T>>> map){
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
