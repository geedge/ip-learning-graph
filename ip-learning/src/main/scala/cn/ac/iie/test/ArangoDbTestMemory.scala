package cn.ac.iie.test

import cn.ac.iie.utils.ConfigUtils
import com.arangodb.entity.{BaseDocument, BaseEdgeDocument}
import com.arangodb.model.AqlQueryOptions
import com.arangodb.util.MapBuilder
import com.arangodb.{ArangoCursor, ArangoDB}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

object ArangoDbTestMemory {
  private val logger: Logger = LoggerFactory.getLogger(ArangoDbTestMemory.getClass)
  @transient
  var arangoDB: ArangoDB = _


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .config("spark.serializer", Config.SPARK_SERIALIZER)
      .config("spark.network.timeout", "300s")
      .config("spark.sql.shuffle.partitions", Config.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY)
      .master(Config.MASTER)
      .getOrCreate()
    logger.warn("sparkession获取成功！！！")

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
      .option("socket_timeout",Config.CLICKHOUSE_SOCKET_TIMEOUT)
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
    val s_IP_DF = spark.sql(
      """
        select
        	s1_s_ip as IP,
        	s1_s_location_region as location,
        	MIN( recv_time ) AS IP_FIRST_FOUND_TIME,
        	MAX( recv_time ) AS IP_LAST_FOUND_TIME,
        	COUNT( * ) AS IP_COUNT_TOTAL
         from global_temp.media_expire_patch
         GROUP BY
        	IP,
        	location
      """.stripMargin)

    val d_IP_DF = spark.sql(
      """
        select
        	s1_d_ip as IP,
        	s1_d_location_region as location,
        	MIN( recv_time ) AS IP_FIRST_FOUND_TIME,
        	MAX( recv_time ) AS IP_LAST_FOUND_TIME,
        	COUNT( * ) AS IP_COUNT_TOTAL
         from global_temp.media_expire_patch
         GROUP BY
        	IP,
        	location
      """.stripMargin)
    import org.apache.spark.sql.functions._
    val v_IP_DF = s_IP_DF.union(d_IP_DF).groupBy("IP", "location").agg(
      min("IP_FIRST_FOUND_TIME").as("IP_FIRST_FOUND_TIME"),
      max("IP_LAST_FOUND_TIME").as("IP_LAST_FOUND_TIME"),
      count("IP").as("IP_COUNT_TOTAL")
    )

    val e_Address_v_FQDN_to_v_IP_DF: DataFrame = spark.sql(
      """
        |SELECT
        |	media_domain AS V_FQDN,
        |	s1_d_ip AS V_IP,
        |	MIN( recv_time ) AS FIRST_FOUND_TIME,
        |	MAX( recv_time ) AS LAST_FOUND_TIME,
        |	COUNT( * ) AS COUNT_TOTAL
        |FROM
        |	global_temp.media_expire_patch
        |WHERE
        |	( media_domain != '' )
        |	AND ( s1_d_ip != '' )
        |GROUP BY
        |	s1_d_ip,
        |	media_domain
      """.stripMargin)

    val e_Visit_v_IP_to_v_FQDN_DF = spark.sql(
      """
        |SELECT
        |	s1_s_ip AS V_IP,
        |	media_domain AS V_FQDN,
        |	MIN( recv_time ) AS FIRST_FOUND_TIME,
        |	MAX( recv_time ) AS LAST_FOUND_TIME,
        |	COUNT( * ) AS COUNT_TOTAL
        |FROM
        |	global_temp.media_expire_patch
        |WHERE
        |	( s1_s_ip != '' )
        |	AND ( media_domain != '' )
        |GROUP BY
        |	s1_s_ip,
        |	media_domain
      """.stripMargin)


    /**
      * 获取数据库连接
      */
    arangoDB = new ArangoDB.Builder()
      .maxConnections(Config.MAXPOOLSIZE)
      .host(ConfigUtils.ARANGODB_HOST, ConfigUtils.ARANGODB_PORT)
      .user(ConfigUtils.ARANGODB_USER)
      .password(ConfigUtils.ARANGODB_PASSWORD)
      .build
    val bindVars = new MapBuilder().get
    val options = new AqlQueryOptions()
        .ttl(Config.ARANGODB_TTL)

    val v_FQDN_Mutabal_Map = new java.util.HashMap[String,BaseDocument](1048576,0.9f)
    val v_IP_Mutabal_Map = new java.util.HashMap[String,BaseDocument](16048576,0.9f)
    val e_FQDN_Address_IP_Mutabal_Map = new java.util.HashMap[String,BaseEdgeDocument](1048576,0.9f)
    val e_IP_Visit_FQDN_Mutabal_Map = new java.util.HashMap[String,BaseEdgeDocument](30408576,0.9f)

    /**
      * 更新FQDN点
      */
    val v_FQDN_Query = "FOR doc IN V_FQDN RETURN doc"
    val v_FQDN_Cursor: ArangoCursor[BaseDocument] = arangoDB.db(ConfigUtils.ARANGODB_DB_NAME)
      .query(v_FQDN_Query, bindVars, options, classOf[BaseDocument])
    while (v_FQDN_Cursor.hasNext){
      val document = v_FQDN_Cursor.next()
      v_FQDN_Mutabal_Map.put(document.getKey,document)
    }
    val v_FQDN_Map= spark.sparkContext.broadcast(v_FQDN_Mutabal_Map)
    v_FQDN_Mutabal_Map.clear()
    v_FQDN_DF.show(20)
    v_FQDN_DF.printSchema()
    v_FQDN_DF.coalesce(Config.REPARTITION_NUMBER).foreachPartition(iter => {
      val v_FQDN_Coll = arangoDB.db(ConfigUtils.ARANGODB_DB_NAME).collection("V_FQDN")
      val docs_Insert = new java.util.ArrayList[BaseDocument]()
      val docs_Update = new java.util.ArrayList[BaseDocument]()
      var i = 0
      iter.foreach(row => {
        val fqdn = row.getAs[String]("FQDN_NAME")
        val v_Fqdn_First = row.getAs[Long]("FQDN_FIRST_FOUND_TIME")
        val v_Fqdn_Last = row.getAs[Long]("FQDN_LAST_FOUND_TIME")
        val v_Fqdn_Cnt = row.getAs[Long]("FQDN_COUNT_TOTAL")

        val doc = v_FQDN_Map.value.getOrDefault(fqdn, null)
        if (doc != null)  {
          val document: BaseDocument = doc
          val fqdn_Cnt = Try(document.getAttribute("FQDN_COUNT_TOTAL")).getOrElse(0).toString.toInt
          document.addAttribute("FQDN_COUNT_TOTAL", fqdn_Cnt)
          document.addAttribute("LAST_FOUND_TIME", v_Fqdn_Last)
          docs_Update.add(document)
        } else {
          val baseDocument: BaseDocument = new BaseDocument()
          baseDocument.setKey(fqdn)
          baseDocument.addAttribute("FQDN_NAME", fqdn)
          baseDocument.addAttribute("FIRST_FOUND_TIME", v_Fqdn_First)
          baseDocument.addAttribute("LAST_FOUND_TIME", v_Fqdn_Last)
          baseDocument.addAttribute("FQDN_COUNT_TOTAL", v_Fqdn_Cnt)
          docs_Insert.add(baseDocument)
        }
      })
//      Try(v_FQDN_Coll.importDocuments(docs_Insert))
      v_FQDN_Coll.importDocuments(docs_Insert)
      Try(v_FQDN_Coll.updateDocuments(docs_Update))
    })
    v_FQDN_Map.destroy()

    /**
      * 更新IP点
      */
    val v_IP_Query = "FOR doc IN V_IP RETURN doc"
    val v_IP_Cursor: ArangoCursor[BaseDocument] = arangoDB.db(ConfigUtils.ARANGODB_DB_NAME)
      .query(v_IP_Query, bindVars, options, classOf[BaseDocument])
    while (v_IP_Cursor.hasNext){
      val document = v_IP_Cursor.next()
      v_IP_Mutabal_Map.put(document.getKey ,document)
    }
    val v_IP_Map = spark.sparkContext.broadcast(v_IP_Mutabal_Map)
//    val v_IP_Map = v_IP_Mutabal_Map.toMap
    v_IP_Mutabal_Map.clear()
    v_IP_DF.printSchema()
    v_IP_DF.coalesce(Config.REPARTITION_NUMBER).foreachPartition(iter => {
      val v_IP_Coll = arangoDB.db(ConfigUtils.ARANGODB_DB_NAME).collection("V_IP")
      val docs_Insert: java.util.ArrayList[BaseDocument] = new java.util.ArrayList[BaseDocument]()
      val docs_Update: java.util.ArrayList[BaseDocument] = new java.util.ArrayList[BaseDocument]()
      var i = 0

      iter.foreach(row => {
        val ip = row.getAs[String]("IP")
        val location = row.getAs[String]("location")
        val v_IP_First = row.getAs[Long]("IP_FIRST_FOUND_TIME")
        val v_IP_Last = row.getAs[Long]("IP_LAST_FOUND_TIME")
        val v_IP_Cnt = row.getAs[Long]("IP_COUNT_TOTAL")
        val doc = v_IP_Map.value.getOrDefault(ip, null)

        if (doc != null) {
          val document: BaseDocument = doc
          val ip_Cnt = Try(document.getAttribute("IP_APPEAR_COUNT")).getOrElse(0).toString.toInt
          document.addAttribute("LAST_FOUND_TIME", v_IP_Last)
          document.addAttribute("IP_APPEAR_COUNT", v_IP_Cnt+ip_Cnt)
          docs_Update.add(document)
        } else {
          val baseDocument: BaseDocument = new BaseDocument()
          baseDocument.setKey(ip)
          baseDocument.addAttribute("IP", ip)
          baseDocument.addAttribute("IP_LOCATION", location)
          baseDocument.addAttribute("FIRST_FOUND_TIME", v_IP_First)
          baseDocument.addAttribute("LAST_FOUND_TIME", v_IP_Last)
          baseDocument.addAttribute("IP_APPEAR_COUNT", v_IP_Cnt)
          docs_Insert.add(baseDocument)
        }
      })
      Try(v_IP_Coll.importDocuments(docs_Insert))
      Try(v_IP_Coll.updateDocuments(docs_Update))
    })
    v_IP_Map.destroy()


    /**
      * 统计e_Address_Fqdn_to_IP
      */
    val e_FQDN_Address_IP_Query = "FOR doc IN E_ADDRESS_V_FQDN_TO_V_IP RETURN doc"
    val e_FQDN_Address_IP_Cursor: ArangoCursor[BaseEdgeDocument] = arangoDB.db(ConfigUtils.ARANGODB_DB_NAME)
      .query(e_FQDN_Address_IP_Query, bindVars, options, classOf[BaseEdgeDocument])
    while (e_FQDN_Address_IP_Cursor.hasNext){
      val document = e_FQDN_Address_IP_Cursor.next()
      e_FQDN_Address_IP_Mutabal_Map.put(document.getKey ,document)
    }
    val e_FQDN_Address_IP_Map = spark.sparkContext.broadcast(e_FQDN_Address_IP_Mutabal_Map)
    e_FQDN_Address_IP_Mutabal_Map.clear()
    e_Address_v_FQDN_to_v_IP_DF.printSchema()
    e_Address_v_FQDN_to_v_IP_DF.coalesce(Config.REPARTITION_NUMBER).foreachPartition(iter => {
      val e_Add_Fqdn_to_IP_Coll = arangoDB.db(ConfigUtils.ARANGODB_DB_NAME).collection("E_ADDRESS_V_FQDN_TO_V_IP")
      val docs_Insert: java.util.ArrayList[BaseEdgeDocument] = new java.util.ArrayList[BaseEdgeDocument]()
      val docs_Update: java.util.ArrayList[BaseEdgeDocument] = new java.util.ArrayList[BaseEdgeDocument]()
      var i = 0
      iter.foreach(row => {
        val fqdn = row.getAs[String]("V_FQDN")
        val ip = row.getAs[String]("V_IP")
        val e_First = row.getAs[Long]("FIRST_FOUND_TIME")
        val e_Last = row.getAs[Long]("LAST_FOUND_TIME")
        val e_Cnt = row.getAs[Long]("COUNT_TOTAL")
        val doc = e_FQDN_Address_IP_Map.value.getOrDefault(fqdn+"-"+ip, null)
        if (doc != null) {
          val document: BaseEdgeDocument = doc
          val e_new_Cnt = Try(document.getAttribute("COUNT_TOTAL")).getOrElse(0).toString.toInt
          document.setFrom(s"V_FQDN/$fqdn")
          document.setTo(s"V_IP/$ip")
          document.addAttribute("LAST_FOUND_TIME", e_Last)
          document.addAttribute("COUNT_TOTAL", e_new_Cnt+e_Cnt)
          docs_Update.add(document)
        } else {
          val baseDocument: BaseEdgeDocument = new BaseEdgeDocument()
          baseDocument.setKey(fqdn+"-"+ip)
          baseDocument.setFrom(s"V_FQDN/$fqdn")
          baseDocument.setTo(s"V_IP/$ip")
          baseDocument.addAttribute("COUNT_TOTAL",e_Cnt)
          baseDocument.addAttribute("FIRST_FOUND_TIME",e_First)
          baseDocument.addAttribute("LAST_FOUND_TIME",e_Last)
          docs_Insert.add(baseDocument)
        }
      })
      Try(e_Add_Fqdn_to_IP_Coll.importDocuments(docs_Insert))
      Try(e_Add_Fqdn_to_IP_Coll.updateDocuments(docs_Update))
    })
    e_FQDN_Address_IP_Map.destroy()


    /**
      * 统计e_Visit_v_IP_to_v_FQDN
      */
    val e_IP_Visit_FQDN_Query = "FOR doc IN E_VISIT_V_IP_TO_V_FQDN RETURN doc"
    val e_IP_Visit_FQDN_Cursor: ArangoCursor[BaseEdgeDocument] = arangoDB.db(ConfigUtils.ARANGODB_DB_NAME)
      .query(e_IP_Visit_FQDN_Query, bindVars, options, classOf[BaseEdgeDocument])
    while (e_IP_Visit_FQDN_Cursor.hasNext){
      val document = e_IP_Visit_FQDN_Cursor.next()
      e_IP_Visit_FQDN_Mutabal_Map.put(document.getKey ,document)
    }
    val e_IP_Visit_FQDN_Map = spark.sparkContext.broadcast(e_IP_Visit_FQDN_Mutabal_Map)

    e_IP_Visit_FQDN_Mutabal_Map.clear()
    e_Visit_v_IP_to_v_FQDN_DF.printSchema()
    e_Visit_v_IP_to_v_FQDN_DF.coalesce(Config.REPARTITION_NUMBER).foreachPartition(iter => {
      val e_Visit_Fqdn_to_IP_Coll = arangoDB.db(ConfigUtils.ARANGODB_DB_NAME).collection("E_VISIT_V_IP_TO_V_FQDN")
      val docs_Insert: java.util.ArrayList[BaseEdgeDocument] = new java.util.ArrayList[BaseEdgeDocument]()
      val docs_Update: java.util.ArrayList[BaseEdgeDocument] = new java.util.ArrayList[BaseEdgeDocument]()
      var i = 0
      iter.foreach(row => {
        val fqdn = row.getAs[String]("V_FQDN")
        val ip = row.getAs[String]("V_IP")
        val e_First = row.getAs[Long]("FIRST_FOUND_TIME")
        val e_Last = row.getAs[Long]("LAST_FOUND_TIME")
        val e_Cnt = row.getAs[Long]("COUNT_TOTAL")
        val doc = e_IP_Visit_FQDN_Map.value.getOrDefault(ip+"-"+fqdn, null)

        if (doc != null) {
          val document: BaseEdgeDocument = doc
          val e_new_Cnt = Try(document.getAttribute("COUNT_TOTAL")).getOrElse(0).toString.toInt
          document.addAttribute("LAST_FOUND_TIME", e_Last)
          document.addAttribute("COUNT_TOTAL", e_new_Cnt+e_Cnt)
          docs_Update.add(document)
        } else {
          val baseDocument: BaseEdgeDocument = new BaseEdgeDocument()
          baseDocument.setKey(ip+"-"+fqdn)
          baseDocument.setFrom("V_IP/"+ip)
          baseDocument.setTo("V_FQDN/"+fqdn)
          baseDocument.addAttribute("COUNT_TOTAL",e_Cnt)
          baseDocument.addAttribute("FIRST_FOUND_TIME",e_First)
          baseDocument.addAttribute("LAST_FOUND_TIME",e_Last)
          docs_Insert.add(baseDocument)
        }
      })
      Try(e_Visit_Fqdn_to_IP_Coll.importDocuments(docs_Insert))
      Try(e_Visit_Fqdn_to_IP_Coll.updateDocuments(docs_Update))

    })
    e_IP_Visit_FQDN_Map.destroy()
    arangoDB.shutdown()

    spark.close()
  }

}
