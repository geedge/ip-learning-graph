package cn.ac.iie.test

import com.arangodb.ArangoDB
import com.arangodb.entity.{BaseDocument, BaseEdgeDocument}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

object ArangoDbTest {
  private val logger: Logger = LoggerFactory.getLogger(ReadClickhouseTest.getClass)

  @transient
  var arangoDB: ArangoDB = _

  def main(args: Array[String]): Unit = {
    //    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .config("spark.serializer", Config.SPARK_SERIALIZER)
      //      .config("spark.kryo.classesToRegister","com.tinkerpop.blueprints.impls.orient.OrientGraphFactory")
      .config("spark.network.timeout", "300s")
      .config("spark.sql.shuffle.partitions", Config.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY)
      .master(Config.MASTER)

      /*
      .config("spark.driver.host", "192.168.41.79")
      .config("spark.jars", "D:\\GITREPO\\ip-learning\\target\\ip-learning-1.0-SNAPSHOT-jar-with-dependencies.jar")
      .master("spark://192.168.40.119:7077")
      */
      .getOrCreate()
    logger.warn("sparkession获取成功！！！")

    //    val sql = "(select * from av_miner.media_expire_patch_local limit 1000)"

    val mediaDataFrame: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:clickhouse://192.168.40.193:8123")
      //      .option("dbtable", "av_miner.media_expire_patch")
      //      .option("dbtable", "(select * from av_miner.media_expire_patch limit 10)")
      //      .option("dbtable","(select media_domain,recv_time,s1_s_ip,s1_d_ip,s1_s_location_region,s1_d_location_region from av_miner.media_expire_patch where media_domain not LIKE '%\\n%')")
      .option("dbtable", s"(select media_domain,recv_time,s1_s_ip,s1_d_ip,s1_s_location_region,s1_d_location_region from av_miner.media_expire_patch where recv_time>=${Config.MINTIME} and recv_time<=${Config.MAXTIME})")
      //      .option("dbtable","(select media_domain,s1_s_ip,s1_d_ip,s1_s_location_region,s1_d_location_region,min(recv_time) as min_recv_time,max(recv_time) as max_recv_time from av_miner.media_expire_patch group by media_domain,s1_s_ip,s1_d_ip,s1_s_location_region,s1_d_location_region limit 10)")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("user", "default")
      .option("password", "111111")
      .option("numPartitions", Config.NUMPARTITIONS)
      .option("partitionColumn", "recv_time")
      .option("lowerBound", Config.MINTIME)
      .option("upperBound", Config.MAXTIME)
      .option("fetchsize", Config.SPARK_SQL_READ_FETCHSIZE)
      .load()
    //    mediaDataFrame.printSchema()
    mediaDataFrame.createOrReplaceGlobalTempView("media_expire_patch")
    //    val mediaDataGlobalView = spark.sql("select * from global_temp.media_expire_patch limit 10")
    //    mediaDataGlobalView.show()

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

    val e_Address_v_FQDN_to_v_IP_DF = spark.sql(
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
      .host("192.168.40.127", 8529)
      .user("root")
      .password("111111")
      .build


    /**
      * 更新FQDN点
      */
    v_FQDN_DF.printSchema()
    v_FQDN_DF.foreachPartition(iter => {
      val v_FQDN_Coll = arangoDB.db("insert_iplearn_index").collection("V_FQDN")
      val docs_Insert = new java.util.ArrayList[BaseDocument]()
      val docs_Update = new java.util.ArrayList[BaseDocument]()
      var i = 0
      iter.foreach(row => {
        val fqdn = row.getAs[String]("FQDN_NAME")
        val v_Fqdn_First = row.getAs[Long]("FQDN_FIRST_FOUND_TIME")
        val v_Fqdn_Last = row.getAs[Long]("FQDN_LAST_FOUND_TIME")
        val v_Fqdn_Cnt = row.getAs[Long]("FQDN_COUNT_TOTAL")

        if (v_FQDN_Coll.documentExists(fqdn)) {
          val document: BaseDocument = v_FQDN_Coll.getDocument(fqdn, classOf[BaseDocument])
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
        i+=1
      })
      Try(v_FQDN_Coll.importDocuments(docs_Insert))
      Try(v_FQDN_Coll.updateDocuments(docs_Update))
    })


    /**
      * 更新IP点
      */
    v_IP_DF.printSchema()
    v_IP_DF.foreachPartition(iter => {
      val v_IP_Coll = arangoDB.db("insert_iplearn_index").collection("V_IP")
      val docs_Insert: java.util.ArrayList[BaseDocument] = new java.util.ArrayList[BaseDocument]()
      val docs_Update: java.util.ArrayList[BaseDocument] = new java.util.ArrayList[BaseDocument]()
      var i = 0

      iter.foreach(row => {
        val ip = row.getAs[String]("IP")
        val location = row.getAs[String]("location")
        val v_IP_First = row.getAs[Long]("IP_FIRST_FOUND_TIME")
        val v_IP_Last = row.getAs[Long]("IP_LAST_FOUND_TIME")
        val v_IP_Cnt = row.getAs[Long]("IP_COUNT_TOTAL")

        if (v_IP_Coll.documentExists(ip)) {
          val document: BaseDocument = v_IP_Coll.getDocument(ip, classOf[BaseDocument])
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
        i+=1
      })
      Try(v_IP_Coll.importDocuments(docs_Insert))
      Try(v_IP_Coll.updateDocuments(docs_Update))
    })

    /**
      * 统计e_Address_Fqdn_to_IP
      */
    e_Address_v_FQDN_to_v_IP_DF.printSchema()
    e_Address_v_FQDN_to_v_IP_DF.foreachPartition(iter => {
      val e_Add_Fqdn_to_IP_Coll = arangoDB.db("insert_iplearn_index").collection("E_ADDRESS_V_FQDN_TO_V_IP")
      val docs_Insert: java.util.ArrayList[BaseEdgeDocument] = new java.util.ArrayList[BaseEdgeDocument]()
      val docs_Update: java.util.ArrayList[BaseEdgeDocument] = new java.util.ArrayList[BaseEdgeDocument]()
      var i = 0
      iter.foreach(row => {
        val fqdn = row.getAs[String]("V_FQDN")
        val ip = row.getAs[String]("V_IP")
        val e_First = row.getAs[Long]("FIRST_FOUND_TIME")
        val e_Last = row.getAs[Long]("LAST_FOUND_TIME")
        val e_Cnt = row.getAs[Long]("COUNT_TOTAL")

        if (e_Add_Fqdn_to_IP_Coll.documentExists(fqdn+"-"+ip)) {
          val document: BaseEdgeDocument = e_Add_Fqdn_to_IP_Coll.getDocument(fqdn+"-"+ip, classOf[BaseEdgeDocument])
          val e_new_Cnt = Try(document.getAttribute("COUNT_TOTAL")).getOrElse(0).toString.toInt
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
//        println(fqdn+"-"+ip)

      })
      Try(e_Add_Fqdn_to_IP_Coll.importDocuments(docs_Insert))
      Try(e_Add_Fqdn_to_IP_Coll.updateDocuments(docs_Update))
    })

    /**
      * 统计e_Visit_v_IP_to_v_FQDN
      */
    e_Visit_v_IP_to_v_FQDN_DF.printSchema()
    e_Visit_v_IP_to_v_FQDN_DF.foreachPartition(iter => {
      val e_Visit_Fqdn_to_IP_Coll = arangoDB.db("insert_iplearn_index").collection("E_VISIT_V_IP_TO_V_FQDN")
      val docs_Insert: java.util.ArrayList[BaseEdgeDocument] = new java.util.ArrayList[BaseEdgeDocument]()
      val docs_Update: java.util.ArrayList[BaseEdgeDocument] = new java.util.ArrayList[BaseEdgeDocument]()
      var i = 0
      iter.foreach(row => {
        val fqdn = row.getAs[String]("V_FQDN")
        val ip = row.getAs[String]("V_IP")
        val e_First = row.getAs[Long]("FIRST_FOUND_TIME")
        val e_Last = row.getAs[Long]("LAST_FOUND_TIME")
        val e_Cnt = row.getAs[Long]("COUNT_TOTAL")

        if (e_Visit_Fqdn_to_IP_Coll.documentExists(ip+"-"+fqdn)) {
          val document: BaseEdgeDocument = e_Visit_Fqdn_to_IP_Coll.getDocument(ip+"-"+fqdn, classOf[BaseEdgeDocument])
          val e_new_Cnt = Try(document.getAttribute("COUNT_TOTAL")).getOrElse(0).toString.toInt
          document.addAttribute("LAST_FOUND_TIME", e_Last)
          document.addAttribute("COUNT_TOTAL", e_new_Cnt+e_Cnt)
          docs_Update.add(document)
        } else {
          val baseDocument: BaseEdgeDocument = new BaseEdgeDocument()
          baseDocument.setKey(ip+"-"+fqdn)
          baseDocument.setFrom(s"V_IP/$ip")
          baseDocument.setTo(s"V_FQDN/$fqdn")
          baseDocument.addAttribute("COUNT_TOTAL",e_Cnt)
          baseDocument.addAttribute("FIRST_FOUND_TIME",e_First)
          baseDocument.addAttribute("LAST_FOUND_TIME",e_Last)
          docs_Insert.add(baseDocument)
        }
        i+=1
      })
      Try(e_Visit_Fqdn_to_IP_Coll.importDocuments(docs_Insert))
      Try(e_Visit_Fqdn_to_IP_Coll.updateDocuments(docs_Update))

    })

    arangoDB.shutdown()
  }


}
