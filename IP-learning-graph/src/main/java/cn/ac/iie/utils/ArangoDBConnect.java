package cn.ac.iie.utils;

import cn.ac.iie.config.ApplicationConfig;
import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.entity.ErrorEntity;
import com.arangodb.entity.MultiDocumentEntity;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.model.DocumentCreateOptions;
import com.arangodb.util.MapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class ArangoDBConnect {
    private static final Logger LOG = LoggerFactory.getLogger(ArangoDBConnect.class);
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

    private ArangoDatabase getDatabase(){
        return arangoDB.db(ApplicationConfig.ARANGODB_DB_NAME);
    }

    public void clean(){
        try {
            if (arangoDB != null){
                arangoDB.shutdown();
            }
        }catch (Exception e){
            LOG.error(e.getMessage());
        }
    }

    public <T> ArangoCursor<T> executorQuery(String query,Class<T> type){
        ArangoDatabase database = getDatabase();
        Map<String, Object> bindVars = new MapBuilder().get();
        AqlQueryOptions options = new AqlQueryOptions()
                .ttl(ApplicationConfig.ARANGODB_TTL);
        try {
            return database.query(query, bindVars, options, type);
        }catch (Exception e){
            LOG.error(e.getMessage());
            return null;
        }finally {
            bindVars.clear();
        }
    }

    @Deprecated
    public <T> void insertAndUpdate(ArrayList<T> docInsert,ArrayList<T> docUpdate,String collectionName){
        ArangoDatabase database = getDatabase();
        try {
            ArangoCollection collection = database.collection(collectionName);
            if (!docInsert.isEmpty()){
                DocumentCreateOptions documentCreateOptions = new DocumentCreateOptions();
//                documentCreateOptions.overwrite(true);
                collection.insertDocuments(docInsert, documentCreateOptions);
            }
            if (!docUpdate.isEmpty()){
                collection.replaceDocuments(docUpdate);
            }
        }catch (Exception e){
            LOG.error("更新失败\n"+e.toString());
        }finally {
            docInsert.clear();
            docInsert.clear();
        }
    }

    public <T> void overwrite(ArrayList<T> docOverwrite,String collectionName){
        ArangoDatabase database = getDatabase();
        try {
            ArangoCollection collection = database.collection(collectionName);
            if (!docOverwrite.isEmpty()){
                DocumentCreateOptions documentCreateOptions = new DocumentCreateOptions();
                documentCreateOptions.overwrite(true);
                documentCreateOptions.silent(true);
                MultiDocumentEntity<DocumentCreateEntity<T>> documentCreateEntityMultiDocumentEntity = collection.insertDocuments(docOverwrite, documentCreateOptions);
                Collection<ErrorEntity> errors = documentCreateEntityMultiDocumentEntity.getErrors();
                for (ErrorEntity errorEntity:errors){
                    LOG.error("写入arangoDB异常："+errorEntity.getErrorMessage());
                }
            }
        }catch (Exception e){
            LOG.error("更新失败："+e.toString());
//            clean();
        }finally {
            docOverwrite.clear();
        }
    }

}
