package cn.ac.iie;

import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.ArrayList;
import java.util.List;

public class TestArango {
    public static void main(String[] args) {
        ArangoDBConnect instance = ArangoDBConnect.getInstance();
        /*
        String query = "FOR doc IN IP filter doc.FIRST_FOUND_TIME >= 1592996080 and doc.FIRST_FOUND_TIME <= 1593112913 RETURN doc";
        ArangoCursor<BaseEdgeDocument> baseEdgeDocuments = instance.executorQuery(query, BaseEdgeDocument.class);
        while (baseEdgeDocuments.hasNext()){
            BaseEdgeDocument next = baseEdgeDocuments.next();
            System.out.println(next.toString());
        }
        */
        BaseEdgeDocument baseEdgeDocument = new BaseEdgeDocument();
        baseEdgeDocument.setKey("192.168.50.6-www.liftopia.com");
        baseEdgeDocument.setFrom("IP/192.168.50.6");
        baseEdgeDocument.setTo("FQDN/www.liftopia.com");
        baseEdgeDocument.addAttribute("HTTP_CNT_TOTAL",3L);
        baseEdgeDocument.addAttribute("DNS_CNT_RECENT",new long[24]);
        baseEdgeDocument.addAttribute("PROTOCOL_TYPE","HTTP");

        ArrayList<BaseEdgeDocument> baseEdgeDocuments = new ArrayList<>();

        baseEdgeDocuments.add(baseEdgeDocument);
        instance.overwrite(baseEdgeDocuments,"R_LOCATE_FQDN2IP");

        instance.clean();
    }
}
