package cn.ac.iie.test

import com.arangodb.entity.{BaseDocument, BaseEdgeDocument}
import com.arangodb.util.MapBuilder
import com.arangodb.{ArangoCursor, ArangoDB}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object TestIndices {
  @transient
  var arangoDB: ArangoDB = _
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.network.timeout", "300s")
      .config("spark.sql.shuffle.partitions", Config.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY)
      .master(Config.MASTER)
      .getOrCreate()

    val mediaDataFrame: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:clickhouse://192.168.40.193:8123")
      .option("dbtable", s"(select media_domain,recv_time,s1_s_ip,s1_d_ip,s1_s_location_region,s1_d_location_region from av_miner.media_expire_patch where recv_time>=${Config.MINTIME} and recv_time<=${Config.MAXTIME})")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("user", "default")
      .option("password", "111111")
      .option("numPartitions", Config.NUMPARTITIONS)
      .option("partitionColumn", "recv_time")
      .option("lowerBound", Config.MINTIME)
      .option("upperBound", Config.MAXTIME)
      .option("fetchsize", Config.SPARK_SQL_READ_FETCHSIZE)
      .load()
    mediaDataFrame.printSchema()
    mediaDataFrame.createOrReplaceGlobalTempView("media_expire_patch")

    val v_FQDN_DF = spark.sql(
      """
        |SELECT
        |	media_domain AS FQDN_NAME,
        |	MIN( recv_time ) AS FQDN_FIRST_FOUND_TIME,
        |	MAX( recv_time ) AS FQDN_LAST_FOUND_TIME,
        |	COUNT( * ) AS FQDN_COUNT_TOTAL
        |FROM
        |	global_temp.media_expire_patch
        |WHERE
        |	media_domain != ''
        |GROUP BY
        |	media_domain
      """.stripMargin
    )
    val time1 = System.currentTimeMillis()

    arangoDB = new ArangoDB.Builder()
      .maxConnections(Config.MAXPOOLSIZE)
      .host("192.168.40.127", 8529)
      .user("root")
      .password("111111")
      .build

    val dbName = "insert_iplearn_index"
    val collectionName = "V_FQDN"
    val query = "FOR doc IN " + collectionName + " RETURN doc"
    val bindVars = new MapBuilder().get
    val cursor: ArangoCursor[BaseEdgeDocument] = arangoDB.db(dbName).query(query, bindVars, null, classOf[BaseEdgeDocument])
    var cursor_Map = scala.collection.mutable.HashMap[String,BaseEdgeDocument]()
    while (cursor.hasNext){
      val document = cursor.next()
      cursor_Map += (document.getKey -> document)
    }
    val time2 = System.currentTimeMillis()

    println((time2 - time1)/1000)
    val docs_Insert = new java.util.ArrayList[BaseDocument]()
    val docs_Update = new java.util.ArrayList[BaseDocument]()
    v_FQDN_DF.foreach(row => {
      val fqdn = row.getAs[String]("FQDN_NAME")
      val v_Fqdn_First = row.getAs[Long]("FQDN_FIRST_FOUND_TIME")
      val v_Fqdn_Last = row.getAs[Long]("FQDN_LAST_FOUND_TIME")
      val v_Fqdn_Cnt = row.getAs[Long]("FQDN_COUNT_TOTAL")

      val doc = cursor_Map.getOrElse(fqdn, null)
      if (doc != null)  {
        val document: BaseDocument = doc
        val fqdn_Cnt = Try(document.getAttribute("FQDN_COUNT_TOTAL")).getOrElse(0).toString.toInt
        document.addAttribute("FQDN_COUNT_TOTAL", fqdn_Cnt)
        document.addAttribute("FQDN_LAST_FOUND_TIME", v_Fqdn_Last)
        docs_Update.add(document)
      } else {
        val baseDocument: BaseDocument = new BaseDocument()
        baseDocument.setKey(fqdn)
        baseDocument.addAttribute("FQDN_NAME", fqdn)
        baseDocument.addAttribute("FQDN_FIRST_FOUND_TIME", v_Fqdn_First)
        baseDocument.addAttribute("FQDN_LAST_FOUND_TIME", v_Fqdn_Last)
        baseDocument.addAttribute("FQDN_COUNT_TOTAL", v_Fqdn_Cnt)
        docs_Insert.add(baseDocument)
      }
    })

//    Try(v_FQDN_Coll.importDocuments(docs_Insert))
//    Try(v_FQDN_Coll.updateDocuments(docs_Update))




    /*
    val db = arangoDB.db("insert_iplearn_index")
    val coll = db.collection("E_ADDRESS_V_FQDN_TO_V_IP")
    val docs = new java.util.ArrayList[BaseEdgeDocument]
    val baseEdgeDocument2 = new BaseEdgeDocument
    baseEdgeDocument2.setKey("test_edge_2.com")
    baseEdgeDocument2.setFrom("V_FQDN/test_edge_2_from")
    baseEdgeDocument2.setTo("V_IP/test_edge_2_to")
    baseEdgeDocument2.addAttribute("e_add_test_str", "1Two3")
    baseEdgeDocument2.addAttribute("e_add_test_num", 4321)
    docs.add(baseEdgeDocument2)

    coll.importDocuments(docs)
    arangoDB.shutdown()

*/

    /*
    val uri: String = "remote:192.168.40.127/iplearning-insert"
    val pool = new OPartitionedDatabasePool(uri, "root", "111111", 5, 5)
    factory = new OrientGraphFactory(uri, "root", "111111", pool)
    val graph = factory.getNoTx
    val ip = "23.224.224.163"
    import scala.collection.JavaConversions._
    /*
    for (v: Vertex <- graph.getVertices("v_IP.IP", ip)) {
      val update_IP_Last = v.getProperty[Long]("LAST_FOUND_TIME")
      val update_IP_Cnt = v.getProperty[Long]("IP_APPEAR_COUNT")
      val sqlComm = new OCommandSQL(
        s"UPDATE v_IP SET LAST_FOUND_TIME = $update_IP_Last,FQDN_APPEAR_COUNT = 100 "
          + s"WHERE IP == '$ip'")
      Try(graph.command(sqlComm).execute())
      println("update ip:" + ip)
    }
*/
    val v_IP_Obj = graph.addVertex("class:v_IP", Nil: _*)

    v_IP_Obj.setProperty("IP", ip)
    v_IP_Obj.setProperty("IP_LOCATION", "fas")
    v_IP_Obj.setProperty("FIRST_FOUND_TIME", 1)
    v_IP_Obj.setProperty("LAST_FOUND_TIME", 1)
    v_IP_Obj.setProperty("IP_APPEAR_COUNT", 1)
*/

    /*
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.network.timeout", "300s")
      .config("spark.sql.shuffle.partitions", Config.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY)
      .config("arangodb.hosts", "192.168.40.127:8529")
      .config("arangodb.user", "root")
      .config("arangodb.password", "111111")
      .master(Config.MASTER)
      .getOrCreate()

    val value: ArangoRDD[BaseDocument] = ArangoSpark
      .load[BaseDocument](spark.sparkContext,"V_FQDN",ReadOptions("insert_iplearn_index"))

//    var stringToDocument: Map[String, BaseDocument] = Map[String,BaseDocument]()
    val lstBuffer: ListBuffer[(String, BaseDocument)] = ListBuffer[(String, BaseDocument)]()
    val map: Map[String, BaseDocument] = value.map(doc => (doc.getKey,doc)).collect().toMap
    println(map.size)

    spark.close()
*/
    /*
    arangoDB = new ArangoDB.Builder()
      .maxConnections(10)
      .host("192.168.40.127", 8529)
      .user("root")
      .password("111111")
      .build

    val db = arangoDB.db("insert_iplearn_index")
//    db.createCollection("V_FQDN")
//    db.createCollection("V_IP")
//    db.createCollection("E_ADDRESS_V_FQDN_TO_V_IP")
//    db.createCollection("E_VISIT_V_IP_TO_V_FQDN")
    val v_FQDN_Coll = db.collection("E_VISIT_V_IP_TO_V_FQDN")
*/

//    val coll: ArangoCollection = db.collection("V_FQDN")
//    val value = coll.getDocument("test1.com",classOf[BaseDocument])
//    val str = value.getAttribute("v_fqdn_test_str")
//    val num: Int = value.getAttribute("v_fqdn_test_num").toString.toInt
//    println(str+"-"+num)
    /*
    val docs = new util.ArrayList[BaseDocument]
    val baseDocument1 = new BaseDocument
    baseDocument1.setKey("test1.com")
    baseDocument1.addAttribute("v_fqdn_test_str", "one2three")
    baseDocument1.addAttribute("v_fqdn_test_num", 1234)
    docs.add(baseDocument1)

    val baseDocument2 = new BaseDocument
    baseDocument2.setKey("test2.com")
    baseDocument2.addAttribute("v_fqdn_test_str", "1Two3")
    baseDocument2.addAttribute("v_fqdn_test_num", 4321)
    docs.add(baseDocument2)
    coll.importDocuments(docs)
*/

//    arangoDB.shutdown()


  }

}
