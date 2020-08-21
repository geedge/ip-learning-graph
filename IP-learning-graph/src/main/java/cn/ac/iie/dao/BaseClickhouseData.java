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
import java.util.function.Function;
import java.util.function.Supplier;

import static cn.ac.iie.service.ingestion.ReadClickhouseData.*;

/**
 * 读取clickhouse数据，封装到map
 * @author wlh
 */
public class BaseClickhouseData {
    private static final Logger LOG = LoggerFactory.getLogger(BaseClickhouseData.class);

    private static ClickhouseConnect manger = ClickhouseConnect.getInstance();
    static HashMap<Integer, HashMap<String, ArrayList<BaseDocument>>> newVertexFqdnMap = new HashMap<>();
    static HashMap<Integer, HashMap<String, ArrayList<BaseDocument>>> newVertexIpMap = new HashMap<>();
    static HashMap<Integer, HashMap<String,ArrayList<BaseDocument>>> newVertexSubscriberMap = new HashMap<>();
    static HashMap<Integer, HashMap<String, ArrayList<BaseEdgeDocument>>> newRelationFqdnAddressIpMap = new HashMap<>();
    static HashMap<Integer, HashMap<String, ArrayList<BaseEdgeDocument>>> newRelationIpVisitFqdnMap = new HashMap<>();
    static HashMap<Integer, HashMap<String, ArrayList<BaseEdgeDocument>>> newRelationSubsciberLocateIpMap = new HashMap<>();

    private DruidPooledConnection connection;
    private Statement statement;

    <T extends BaseDocument> void baseDocumentFromClickhouse(HashMap<Integer, HashMap<String, ArrayList<T>>> newMap,
                                                             Supplier<String> getSqlSupplier,
                                                             Function<ResultSet,T> formatResultFunc) {
        long start = System.currentTimeMillis();
        initializeMap(newMap);
        String sql = getSqlSupplier.get();
        try {
            connection = manger.getConnection();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            int i = 0;
            while (resultSet.next()) {
                T newDoc = formatResultFunc.apply(resultSet);
                if (newDoc != null) {
                    i+=1;
                    putMapByHashcode(newDoc, newMap);
                }
            }
            long last = System.currentTimeMillis();
            LOG.info(sql + "\n读取"+i+"条数据，运行时间：" + (last - start));
        }catch (Exception e){
            e.printStackTrace();
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

    public <T extends BaseDocument> HashMap<Integer, HashMap<String,ArrayList<T>>> baseDocFromCk(Supplier<String> getSqlSupplier,
                                                             Function<ResultSet,T> formatResultFunc){
        long start = System.currentTimeMillis();
        HashMap<Integer, HashMap<String, ArrayList<T>>> newDataMap = initializeMap();
        if (newDataMap == null){
            return null;
        }
        String sql = getSqlSupplier.get();
        try {
            connection = manger.getConnection();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            int i = 0;
            while (resultSet.next()) {
                T newDoc = formatResultFunc.apply(resultSet);
                if (newDoc != null) {
                    i+=1;
                    putMapByHashcode(newDoc, newDataMap);
                }
            }
            long last = System.currentTimeMillis();
            LOG.info(sql + "\n读取"+i+"条数据，运行时间：" + (last - start));
        }catch (Exception e){
            e.printStackTrace();
            LOG.error("获取原始数据失败 "+e.toString());
        }finally {
            manger.clear(statement,connection);
        }
        return newDataMap;
    }

    private <T extends BaseDocument> HashMap<Integer, HashMap<String,ArrayList<T>>> initializeMap(){
        try {
            HashMap<Integer, HashMap<String, ArrayList<T>>> newDataMap = new HashMap<>();
            for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
                newDataMap.put(i, new HashMap<>());
            }
            return newDataMap;
        }catch (Exception e){
            e.printStackTrace();
            LOG.error("数据初始化失败 "+e.toString());
            return null;
        }

    }


}
