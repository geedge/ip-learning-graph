package cn.ac.iie;

import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseEdgeDocument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestList {
    public static void main(String[] args) {
        /*
        ArangoDBConnect arangoConnect = ArangoDBConnect.getInstance();
        ArangoCursor<BaseEdgeDocument> documents = arangoConnect.executorQuery("FOR doc IN R_LOCATE_FQDN2IP filter doc.FIRST_FOUND_TIME >= 1596080839 and doc.FIRST_FOUND_TIME <= 1596395473  RETURN doc", BaseEdgeDocument.class);
        List<BaseEdgeDocument> baseEdgeDocuments = documents.asListRemaining();
        for (BaseEdgeDocument doc: baseEdgeDocuments){
            doc.updateAttribute("PROTOCOL_TYPE","123");
        }

*/

        ArrayList<Integer> integers = new ArrayList<>();
        integers.add(10);
        integers.add(8);
        integers.add(11);
        integers.add(5);
        integers.add(5);
        integers.add(4);
        integers.add(4);
        integers.add(12);

        Integer[] objects = integers.toArray(new Integer[2]);
        System.out.println(Arrays.toString(objects));


        Collections.sort(integers);
        System.out.println(integers);
        integers.add(5);
        Collections.sort(integers);
        System.out.println(integers);
        Collections.reverse(integers);
        System.out.println(integers);
        System.out.println(integers.indexOf(5));

    }
}
