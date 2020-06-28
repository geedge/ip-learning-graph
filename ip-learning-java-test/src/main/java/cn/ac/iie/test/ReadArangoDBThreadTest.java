package cn.ac.iie.test;

import cn.ac.iie.config.ApplicationConfig;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.util.MapBuilder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ReadArangoDBThreadTest {

    private static ConcurrentHashMap<String,BaseDocument> fqdnMap = new ConcurrentHashMap<String,BaseDocument>();
    public static void main(String[] args) throws Exception {
        final ArangoDB arangoDB = new ArangoDB.Builder()
                .maxConnections(ApplicationConfig.THREAD_POOL_NUMBER)
                .host(ApplicationConfig.ARANGODB_HOST, ApplicationConfig.ARANGODB_PORT)
                .user(ApplicationConfig.ARANGODB_USER)
                .password(ApplicationConfig.ARANGODB_PASSWORD)
                .build();
        Map<String, Object> bindVars = new MapBuilder().get();
        AqlQueryOptions options = new AqlQueryOptions().ttl(ApplicationConfig.ARANGODB_TTL);

        String sql = "LET FQDN = (FOR doc IN V_FQDN RETURN doc) return {max_time:MAX(FQDN[*].FQDN_FIRST_FOUND_TIME),min_time:MIN(FQDN[*].FQDN_FIRST_FOUND_TIME)}";
//        String sql = "LET IP = (FOR doc IN V_IP RETURN doc) return {max_time:MAX(IP[*].FIRST_FOUND_TIME),min_time:MIN(IP[*].FIRST_FOUND_TIME)}";
        final ArangoDatabase db = arangoDB.db("insert_iplearn_index");
        long startTime = System.currentTimeMillis();
        ArangoCursor<BaseDocument> timeDoc = db.query(sql, bindVars, options, BaseDocument.class);
        long maxTime =0L;
        long minTime =0L;
        while (timeDoc.hasNext()){
            BaseDocument doc = timeDoc.next();
            maxTime = Long.parseLong(doc.getAttribute("max_time").toString()) + ApplicationConfig.THREAD_POOL_NUMBER;
            minTime = Long.parseLong(doc.getAttribute("min_time").toString());
        }
        long lastTime = System.currentTimeMillis();
        System.out.println("查询最大最小时间用时："+(lastTime-startTime));
        System.out.println(maxTime + "--" + minTime);
        final long diffTime = (maxTime - minTime) / ApplicationConfig.THREAD_POOL_NUMBER;
        ExecutorService pool = Executors.newFixedThreadPool(ApplicationConfig.THREAD_POOL_NUMBER);

        for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
//            long finalMaxTime = maxTime;
            final long finalMinTime = minTime;
            pool.execute(new Runnable() {

                public void run() {
                    String name = Thread.currentThread().getName();
                    ArangoDatabase insert_iplearn_index = arangoDB.db("insert_iplearn_index");
                    Map<String, Object> bindVars = new MapBuilder().get();
                    AqlQueryOptions options = new AqlQueryOptions().ttl(ApplicationConfig.ARANGODB_TTL);
                    String[] split = name.split("-");
                    Long threadNum = Long.parseLong(split[3]);
                    long maxThreadTime = finalMinTime + threadNum * diffTime;
                    long minThreadTime = finalMinTime + (threadNum-1)*diffTime;
                    String query = "FOR doc IN V_FQDN filter doc.FQDN_FIRST_FOUND_TIME >= "+minThreadTime+" and doc.FQDN_FIRST_FOUND_TIME <= "+maxThreadTime+" RETURN doc";
//                    String query = "FOR doc IN V_IP filter doc.FIRST_FOUND_TIME >= "+minThreadTime+" and doc.FIRST_FOUND_TIME <= "+maxThreadTime+" RETURN doc";
                    System.out.println(name+":"+query);
                    long s = System.currentTimeMillis();
                    ArangoCursor<BaseDocument> fqdnDoc = insert_iplearn_index.query(query, bindVars, options, BaseDocument.class);
                    List<BaseDocument> baseDocuments = fqdnDoc.asListRemaining();
                    int i = 0;
                    for (BaseDocument doc:baseDocuments){
                        String key = doc.getKey();
//                        System.out.println(name+":"+key);
                        fqdnMap.put(key,doc);
                        i++;
                    }
                    /*
                    while (fqdnDoc.hasNext()){
                        BaseDocument doc = fqdnDoc.next();
                    }
                    */
                    System.out.println(name+":"+ i);
                    long l = System.currentTimeMillis();
                    System.out.println(name+"运行时间："+(l-s));
                }
            });
        }
        pool.shutdown();
        while (!pool.awaitTermination(20, TimeUnit.SECONDS)){

        }
        System.out.println(fqdnMap.size());
        arangoDB.shutdown();

    }
}
