package cn.ac.iie;

import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.List;

public class TestArango {
    public static void main(String[] args) {
        ArangoDBConnect instance = ArangoDBConnect.getInstance();
        String query = "FOR doc IN IP filter doc.FIRST_FOUND_TIME >= 1592996080 and doc.FIRST_FOUND_TIME <= 1593112913 RETURN doc";
        ArangoCursor<BaseEdgeDocument> baseEdgeDocuments = instance.executorQuery(query, BaseEdgeDocument.class);
        while (baseEdgeDocuments.hasNext()){
            BaseEdgeDocument next = baseEdgeDocuments.next();
            System.out.println(next.toString());
        }
//        ArangoDBConnect.clean();
    }
}
