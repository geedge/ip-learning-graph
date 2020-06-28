package cn.ac.iie.test

import com.arangodb.entity.BaseDocument
import com.arangodb.model.AqlQueryOptions
import com.arangodb.util.MapBuilder
import com.arangodb.{ArangoCursor, ArangoDB}

object ArangoDbReadV_IPTest {
  @transient
  var arangoDB: ArangoDB = _

  def main(args: Array[String]): Unit = {
    arangoDB = new ArangoDB.Builder()
      .maxConnections(Config.MAXPOOLSIZE)
      .host("192.168.40.127", 8529)
      .user("root")
      .password("111111")
      .build
    val bindVars = new MapBuilder().get
    val options = new AqlQueryOptions()
      .ttl(Config.ARANGODB_TTL)
    val v_IP_Mutabal_Map = new java.util.HashMap[String,BaseDocument](16048576,0.9f)
    val v_IP_Query = "FOR doc IN V_IP RETURN doc"
    val v_IP_Cursor: ArangoCursor[BaseDocument] = arangoDB.db("insert_iplearn_index")
      .query(v_IP_Query, bindVars, options, classOf[BaseDocument])

    while (v_IP_Cursor.hasNext){
      val document = v_IP_Cursor.next()
      v_IP_Mutabal_Map.put(document.getKey ,document)
    }

    println(v_IP_Mutabal_Map.size())
    arangoDB.shutdown()

  }

}
