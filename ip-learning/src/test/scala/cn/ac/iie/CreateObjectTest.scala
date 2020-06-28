package cn.ac.iie

import java.lang

import com.orientechnologies.orient.core.command.OCommandRequest
import com.orientechnologies.orient.core.config.OGlobalConfiguration
import com.orientechnologies.orient.core.db._
import com.orientechnologies.orient.core.metadata.schema.{OClass, OType}
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.{OrientDynaElementIterable, OrientGraph, OrientGraphFactory}

object CreateObjectTest {
  def main(args: Array[String]): Unit = {

    /*
    val orientDbProtocol: String = "remote"
    val orientDbHost: String = "192.168.40.127"
    val orientDbDatabase: String = "iplearning-insert"
    val orientDbUsername: String = "root"
    val orientDbPassword: String = "111111"

    val orientDbUri: String = s"${orientDbProtocol}:${orientDbHost}/${orientDbDatabase}"
    val orientDB: OrientGraph = new OrientGraph(orientDbUri, orientDbUsername, orientDbPassword)

    try {
      println("---------------")
      println(orientDB.isAutoStartTx)
      orientDB.dropEdgeType("author-paper ")
      orientDB.commit()
    }catch {
      //catching errors & print out
      case ex: Exception => println(ex.getMessage)
    }
    finally {
      //close the current OrientDb's connection
      orientDB.shutdown()
    }
    */

    /*
    val poolCfg = OrientDBConfig.builder
    poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MIN, 5)
    poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MAX, 10)

    val pool: ODatabasePool = new ODatabasePool(orientDB, "iplearning-insert", "root", "111111", poolCfg.build)
    val session: ODatabaseSession = pool.acquire()
    println(session.isValidationEnabled)
    session.close()
    */

    /*
    val info = new Properties()
    info.put("user", "root")
    info.put("password", "111111")

    info.put("db.usePool", "true"); // USE THE POOL
    info.put("db.pool.min", "3");   // MINIMUM POOL SIZE


    val conn: OrientJdbcConnection = DriverManager.getConnection("jdbc:orient:remote:192.168.40.127/iplearing-index", info).asInstanceOf[OrientJdbcConnection]
    println(conn.isValid(1))
    */


    val uri: String = "remote:192.168.40.127/iplearning-insert"
    val pool = new OPartitionedDatabasePool(uri,"root","111111",20,20)
    val factory: OrientGraphFactory = new OrientGraphFactory(uri,"root","111111",pool)
//    val graph: OrientGraph = factory.getTx()
    val graph = factory.getNoTx

    /*

    val v_FQDN = graph.createVertexType("v_FQDN")
    v_FQDN.createProperty("FQDN_NAME", OType.STRING)
    v_FQDN.createProperty("FIRST_FOUND_TIME", OType.LONG)
    v_FQDN.createProperty("LAST_FOUND_TIME", OType.LONG)
    v_FQDN.createProperty("FQDN_APPEAR_COUNT", OType.LONG)

    val v_IP = graph.createVertexType("v_IP")
    v_IP.createProperty("IP", OType.STRING)
    v_IP.createProperty("FIRST_FOUND_TIME", OType.LONG)
    v_IP.createProperty("LAST_FOUND_TIME", OType.LONG)
    v_IP.createProperty("IP_APPEAR_COUNT", OType.LONG)

    val e_FQDN_to_IP = graph.createEdgeType("E_ADDRESS_V_FQDN_TO_V_IP")
    e_FQDN_to_IP.createProperty("FIRST_FOUND_TIME", OType.LONG)
    e_FQDN_to_IP.createProperty("LAST_FOUND_TIME", OType.LONG)
    e_FQDN_to_IP.createProperty("COUNT_TOTAL", OType.LONG)

    val e_IP_to_FQDN = graph.createEdgeType("E_VISIT_V_IP_TO_V_FQDN")
    e_IP_to_FQDN.createProperty("FIRST_FOUND_TIME", OType.LONG)
    e_IP_to_FQDN.createProperty("LAST_FOUND_TIME", OType.LONG)
    e_IP_to_FQDN.createProperty("COUNT_TOTAL", OType.LONG)
*/


    /*
    val orientDB = new OrientDB("remote:192.168.40.127", "root", "111111", OrientDBConfig.defaultConfig)

    val poolCfg = OrientDBConfig.builder
    poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MIN, 5)
    poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MAX, 10)

    val pool: ODatabasePool = new ODatabasePool(orientDB, "iplearning-insert", "root", "111111", poolCfg.build)
    val session: ODatabaseSession = pool.acquire()

    println(session.isClosed)
*/

    /*
    val vertex = graph.addVertex("class:v_FQDN",Nil: _*)
    vertex.setProperties("FQDN_NAME","123com1234")
    vertex.setProperty("FIRST_FOUND_TIME", 1571241623)
    vertex.setProperty("LAST_FOUND_TIME", 1571241570)
    vertex.setProperty("FQDN_APPEAR_COUNT", 5)

    val v_IP = graph.addVertex("class:v_IP",Nil: _*)
    v_IP.setProperty("IP","3.3.3.3")
    v_IP.setProperty("IP_LOCATION","cn")
    v_IP.setProperty("FIRST_FOUND_TIME", 1571241623)
    v_IP.setProperty("LAST_FOUND_TIME", 1571241570)
    v_IP.setProperty("IP_APPEAR_COUNT", 6543)

    graph.addEdge(null,vertex,v_IP,"E_ADDRESS_V_FQDN_TO_V_IP")
*/
    val sqlComm = new OCommandSQL(
      "UPDATE E_ADDRESS_V_FQDN_TO_V_IP SET LAST_FOUND_TIME = 1412,FQDN_APPEAR_COUNT = 1244"
        + "UPSERT WHERE 'v5-dy.ixigua.com' IN FQDN_NAME")
    graph.command(sqlComm).execute()

    import scala.collection.JavaConversions._
    /*
    if (graph.getVertices("v_FQDN.FQDN_NAME","v5-dy.ixigua.com111111").nonEmpty){
      for (v <- graph.getVertices("v_FQDN.FQDN_NAME","v5-dy.ixigua.com1")) {
        println(v == null)
      }
    }

    val sqlComm = new OCommandSQL(
      "UPDATE v_FQDN SET LAST_FOUND_TIME = 1412,FQDN_APPEAR_COUNT = 1244"
        + "WHERE 'v5-dy.ixigua.com' IN FQDN_NAME")
  graph.command(sqlComm).execute()
  */


    /*
    println(graphFactory.exists())
    val tx = graphFactory.getTx
    val v_FQDN = tx.createVertexType("V_FQDN")
    v_FQDN.createProperty("FQDN_NAME",OType.STRING)
    v_FQDN.createProperty("FIRST_FOUND_TIME",OType.LONG)
    v_FQDN.createProperty("LAST_FOUND_TIME",OType.LONG)
    */
  }

}
