package cn.ac.iie;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.etl.UpdateEFqdnAddressIp;
import cn.ac.iie.utils.ArangoDBConnect;
import cn.ac.iie.utils.ClickhouseConnect;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.*;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class TestMap {

    public static void main(String[] args) {
        /*
        long maxTime = 1592794800;
        long minTime = 1590112800;
        String where = " common_recv_time >= " + minTime + " AND common_recv_time <= " + maxTime+ " AND (common_schema_type = 'HTTP' or common_schema_type = 'SSL')";
        String sql = "SELECT common_schema_type,http_host,ssl_sni,MAX(common_recv_time) as LAST_FOUND_TIME,MIN(common_recv_time) as FIRST_FOUND_TIME,COUNT(*) as COUNT_TOTAL,groupArray(30)(common_server_ip) as DIST_CIP_RECENT FROM tsg_galaxy_v3.connection_record_log WHERE "+where+" GROUP BY common_schema_type,http_host,ssl_sni";
        System.out.println(sql);
        long start = System.currentTimeMillis();
        try {
            DruidPooledConnection connection = ClickhouseConnect.getInstance().getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            HashMap<String, HashMap<String,Long>> schemaHashMap = new HashMap<>();
            while (resultSet.next()) {
                String[] distCipRecents = (String[]) resultSet.getArray("DIST_CIP_RECENT").getArray();
                for (String s:distCipRecents){
                    System.out.print(s+",");
                }
                System.out.println();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        */
//        long[] longs = new long[]{1,2,3,4,5,6,7};
        /*
        long[] longs = new long[]{1,2,3,4};
        long[] longs1 = new long[7];
        System.arraycopy(longs,0,longs1,1,longs.length-1);
        longs1[0] = 0;
        for (long c:longs1){
            System.out.println(c);
        }
        */


        String[] distCipRecents = new  String[]{"2.3"};
        ArrayList<BaseDocument> baseEdgeDocuments = new ArrayList<>();

        BaseDocument newDoc = new BaseDocument();
        newDoc.setKey("111$#$");
//        newDoc.setKey("11111");
        newDoc.addAttribute("FIRST_FOUND_TIME", 123);
        newDoc.addAttribute("LAST_FOUND_TIME", 123);

        BaseDocument document = new BaseDocument();
        document.setKey("4399pk.com2142379111");
        document.addAttribute("FIRST_FOUND_TIME",1592743297);
        document.addAttribute("LAST_FOUND_TIME",1592743297);

        baseEdgeDocuments.add(newDoc);
        baseEdgeDocuments.add(document);

        ArangoDBConnect instance = ArangoDBConnect.getInstance();

        instance.overwrite(baseEdgeDocuments,"FQDN");

        ArangoDBConnect.clean();



    }
}
