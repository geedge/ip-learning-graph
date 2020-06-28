package cn.ac.iie;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class CreateObjectJavaTest {
    public static void main(String[] args) {
        String orientDbProtocol  = "remote";
        String orientDbHost = "192.168.40.127";
        String orientDbDatabase= "iplearning-insert";
        String orientDbUsername= "root";
        String orientDbPassword= "111111";

//        String orientDbUri  = "${orientDbProtocol}:${orientDbHost}/${orientDbDatabase}";
        String orientDbUri  = orientDbProtocol+":"+orientDbHost+"/"+orientDbDatabase;
        OrientGraph dblpOrientDbGraph  = new OrientGraph(orientDbUri, orientDbUsername, orientDbPassword);

//    val orientDB: OrientGraph = new OrientGraph(orientDbUri,orientDbUsername, orientDbPassword)
        try {
            System.out.println((dblpOrientDbGraph.isAutoStartTx()));
            dblpOrientDbGraph.dropEdgeType("author_of ");
            dblpOrientDbGraph.commit();
        }catch (Exception ex){
            //catching errors & print out
            System.out.println(ex.getMessage());
        }
    finally {
            //close the current OrientDb's connection
            dblpOrientDbGraph.shutdown();
        }

    }
}
