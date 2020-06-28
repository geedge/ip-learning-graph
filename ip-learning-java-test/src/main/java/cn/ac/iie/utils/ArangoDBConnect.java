package cn.ac.iie.utils;

import cn.ac.iie.config.ApplicationConfig;
import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.util.MapBuilder;

import java.util.ArrayList;
import java.util.Map;

public class ArangoDBConnect {
    private static ArangoDB arangoDB = null;
    private static ArangoDBConnect conn = null;
    static {
        getArangoDB();
    }

    private static void getArangoDB(){
        arangoDB = new ArangoDB.Builder()
                .maxConnections(ApplicationConfig.THREAD_POOL_NUMBER)
                .host(ApplicationConfig.ARANGODB_HOST, ApplicationConfig.ARANGODB_PORT)
                .user(ApplicationConfig.ARANGODB_USER)
                .password(ApplicationConfig.ARANGODB_PASSWORD)
                .build();
    }

    public static synchronized ArangoDBConnect getInstance(){
        if (null == conn){
            conn = new ArangoDBConnect();
        }
        return conn;
    }

    public ArangoDatabase getDatabase(){
        return arangoDB.db(ApplicationConfig.ARANGODB_DB_NAME);
    }

    public static void clean(){
        try {
            if (arangoDB != null){
                arangoDB.shutdown();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public <T> ArangoCursor<T> executorQuery(String query,Class<T> type){
        ArangoDatabase database = getDatabase();
        Map<String, Object> bindVars = new MapBuilder().get();
        AqlQueryOptions options = new AqlQueryOptions().ttl(ApplicationConfig.ARANGODB_TTL);
        try {
            return database.query(query, bindVars, options, type);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }finally {
            bindVars.clear();
        }
    }

    public <T> void insertAndUpdate(ArrayList<T> docInsert,ArrayList<T> docUpdate,String collectionName){
        ArangoDatabase database = getDatabase();
        try {
            ArangoCollection collection = database.collection(collectionName);
            if (!docInsert.isEmpty()){
                collection.importDocuments(docInsert);
            }
            if (!docUpdate.isEmpty()){
                collection.replaceDocuments(docUpdate);
            }
        }catch (Exception e){
            System.out.println("更新失败");
            e.printStackTrace();
        }finally {
            docInsert.clear();
            docInsert.clear();
        }
    }

}
