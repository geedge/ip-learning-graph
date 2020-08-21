package cn.ac.iie;

import cn.ac.iie.dao.BaseClickhouseData;
import cn.ac.iie.service.ingestion.ReadClickhouseData;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class TestReadClickhouse {

    private static BaseClickhouseData baseClickhouseData = new BaseClickhouseData();

    @Test
    public void testReadFqdnFromCk(){


        HashMap<Integer, HashMap<String, ArrayList<BaseDocument>>> newData =
                baseClickhouseData.baseDocFromCk(ReadClickhouseData::getVertexFqdnSql,
                        ReadClickhouseData::getVertexFqdnDocument);
        printMap(newData);

    }

    @Test
    public void testReadFqdnLocIpFromCk(){
        HashMap<Integer, HashMap<String, ArrayList<BaseEdgeDocument>>> map =
                baseClickhouseData.baseDocFromCk(ReadClickhouseData::getRelationshipFqdnAddressIpSql,
                        ReadClickhouseData::getRelationFqdnAddressIpDocument);

        printMap(map);
    }


    private<T extends BaseDocument> void printMap(HashMap<Integer, HashMap<String, ArrayList<T>>> newData){
        HashMap<String, ArrayList<T>> map = newData.get(1);
        Set<String> strings = map.keySet();
        for (String key:strings){
            ArrayList<T> baseDocuments = map.get(key);
            System.out.println(baseDocuments.get(0));
        }
    }
}
