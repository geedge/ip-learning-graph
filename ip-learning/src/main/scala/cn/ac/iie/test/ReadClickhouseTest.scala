package cn.ac.iie.test

import com.orientechnologies.orient.core.db.{ODatabasePool, OPartitionedDatabasePool}
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.impls.orient.{OrientGraph, OrientGraphFactory}
import com.tinkerpop.blueprints.{Direction, Edge, Vertex}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

object ReadClickhouseTest {
  private val logger: Logger = LoggerFactory.getLogger(ReadClickhouseTest.getClass)
  @transient
  var factory: OrientGraphFactory = _

  @transient
  var pool: ODatabasePool = _

  def main(args: Array[String]): Unit = {
    //    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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
      .option("dbtable",s"(select media_domain,recv_time,s1_s_ip,s1_d_ip,s1_s_location_region,s1_d_location_region from av_miner.media_expire_patch where recv_time>=${Config.MINTIME} and recv_time<=${Config.MAXTIME})")
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
    val v_IP_DF = s_IP_DF.union(d_IP_DF).groupBy("IP","location").agg(
      min("IP_FIRST_FOUND_TIME") .as("IP_FIRST_FOUND_TIME"),
      max("IP_LAST_FOUND_TIME").as("IP_LAST_FOUND_TIME"),
      count("IP").as("IP_COUNT_TOTAL")
    )

    /*
    val v_IP_DF = spark.sql(
      """
        |SELECT
        |	IP,
        |	location,
        |	MIN( recv_time ) AS IP_FIRST_FOUND_TIME,
        |	MAX( recv_time ) AS IP_LAST_FOUND_TIME,
        |	COUNT( * ) AS IP_COUNT_TOTAL
        |FROM
        |	(
        |	( SELECT s1_s_ip AS IP, s1_s_location_region AS location, recv_time FROM global_temp.media_expire_patch ) UNION ALL
        |	( SELECT s1_d_ip AS IP, s1_d_location_region AS location, recv_time FROM global_temp.media_expire_patch )
        |	)
        |GROUP BY
        |	IP,
        |	location
      """.stripMargin)
      */
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
    val uri: String = "remote:192.168.40.127/iplearning-insert"
//    val uri: String = "remote:192.168.40.207/iplearing-test"
//    val uri: String = "remote:192.168.40.152:2424;192.168.40.151:2424:192.168.40.153:2424/iplearing-test"
    val pool = new OPartitionedDatabasePool(uri, "root", "111111", Config.MAXPOOLSIZE, Config.MAXPOOLSIZE)
    factory = new OrientGraphFactory(uri, "root", "111111", pool)
    factory.setConnectionStrategy("ROUND_ROBIN_CONNECT")

    /**
      * 更新FQDN点
      */
    v_FQDN_DF.printSchema()
    v_FQDN_DF.foreach(row => {
      val fqdn = row.getAs[String]("FQDN_NAME")
      val v_Fqdn_First = row.getAs[Long]("FQDN_FIRST_FOUND_TIME")
      val v_Fqdn_Last = row.getAs[Long]("FQDN_LAST_FOUND_TIME")
      val v_Fqdn_Cnt = row.getAs[Long]("FQDN_COUNT_TOTAL")
//      val graph = factory.getNoTx
      val graph: OrientGraph = factory.getTx
      var v_Fqdn_Obj: Vertex = null
      import scala.collection.JavaConversions._

      if (graph.getVertices("v_FQDN.FQDN_NAME", fqdn).nonEmpty) {
        for (v: Vertex <- graph.getVertices("v_FQDN.FQDN_NAME", fqdn)) {
          val update_Fqdn_Last = v.getProperty[Long]("LAST_FOUND_TIME")
          val update_Fqdn_Cnt = v.getProperty[Long]("FQDN_APPEAR_COUNT")
          val sqlComm = new OCommandSQL(
            s"UPDATE v_FQDN SET LAST_FOUND_TIME = $update_Fqdn_Last,FQDN_APPEAR_COUNT = ${update_Fqdn_Cnt + v_Fqdn_Cnt} WHERE FQDN_NAME == '$fqdn'")
          Try(graph.command(sqlComm).execute())
          println("update fqdn:"+fqdn)
          v_Fqdn_Obj = v
        }
      } else {
        v_Fqdn_Obj = graph.addVertex("class:v_FQDN", Nil: _*)

        v_Fqdn_Obj.setProperty("FQDN_NAME", fqdn)
        v_Fqdn_Obj.setProperty("FIRST_FOUND_TIME", v_Fqdn_First)
        v_Fqdn_Obj.setProperty("LAST_FOUND_TIME", v_Fqdn_Last)
        v_Fqdn_Obj.setProperty("FQDN_APPEAR_COUNT", v_Fqdn_Cnt)
        println("insert fqdn:"+fqdn)
      }
      var i = 0
      i = i+1
      if (i == 10000){
        graph.commit()
      }
    })
    factory.getTx.commit()

    /**
      * 更新IP点
      */
    v_IP_DF.printSchema()
    v_IP_DF.foreach(row => {
      val ip = row.getAs[String]("IP")
      val location = row.getAs[String]("location")
      val v_IP_First = row.getAs[Long]("IP_FIRST_FOUND_TIME")
      val v_IP_Last = row.getAs[Long]("IP_LAST_FOUND_TIME")
      val v_IP_Cnt = row.getAs[Long]("IP_COUNT_TOTAL")
//      val graph = factory.getNoTx

      val graph = factory.getTx
      var v_IP_Obj: Vertex = null
      import scala.collection.JavaConversions._
      if (graph.getVertices("v_IP.IP", ip).nonEmpty) {
        for (v: Vertex <- graph.getVertices("v_IP.IP", ip)) {
          val update_IP_Last = v.getProperty[Long]("LAST_FOUND_TIME")
          val update_IP_Cnt = v.getProperty[Long]("IP_APPEAR_COUNT")
          val sqlComm = new OCommandSQL(
            s"UPDATE v_IP SET LAST_FOUND_TIME = $update_IP_Last,FQDN_APPEAR_COUNT = ${update_IP_Cnt + v_IP_Cnt} "
              + s"WHERE IP == '$ip'")
          Try(graph.command(sqlComm).execute())
          println("update ip:"+ip)
          v_IP_Obj = v
        }
      } else {
        v_IP_Obj = graph.addVertex("class:v_IP", Nil: _*)

        v_IP_Obj.setProperty("IP", ip)
        v_IP_Obj.setProperty("IP_LOCATION", location)
        v_IP_Obj.setProperty("FIRST_FOUND_TIME", v_IP_First)
        v_IP_Obj.setProperty("LAST_FOUND_TIME", v_IP_Last)
        v_IP_Obj.setProperty("IP_APPEAR_COUNT", v_IP_Cnt)
        println("insert ip:"+ip)
      }
      var i = 0
      i = i+1
      if (i == 10000){
        graph.commit()
      }
    })
    factory.getTx.commit()

    /**
      * 统计e_Address_Fqdn_to_IP
      */
    e_Address_v_FQDN_to_v_IP_DF.printSchema()
    e_Address_v_FQDN_to_v_IP_DF.foreach(row => {
      val fqdn = row.getAs[String]("V_FQDN")
      val ip = row.getAs[String]("V_IP")
      val e_First = row.getAs[Long]("FIRST_FOUND_TIME")
      val e_Last = row.getAs[Long]("LAST_FOUND_TIME")
      val e_Cnt = row.getAs[Long]("COUNT_TOTAL")

      val graph = factory.getNoTx
      var v_Fqdn_Obj: Vertex = null
      var v_IP_Obj: Vertex = null
      var e_Edge_Obj:Edge = null

      import scala.collection.JavaConversions._

      //获取fqdn点
      if (graph.getVertices("v_FQDN.FQDN_NAME", fqdn).nonEmpty) {
        for (v: Vertex <- graph.getVertices("v_FQDN.FQDN_NAME", fqdn)) {
          v_Fqdn_Obj = v
        }
      } else {
        v_Fqdn_Obj = graph.addVertex("class:v_FQDN", Nil: _*)
        v_Fqdn_Obj.setProperty("FQDN_NAME", fqdn)
        v_Fqdn_Obj.setProperty("FIRST_FOUND_TIME", 0)
        v_Fqdn_Obj.setProperty("LAST_FOUND_TIME", 0)
        v_Fqdn_Obj.setProperty("FQDN_APPEAR_COUNT", 0)

      }

      //获取IP点
      if (graph.getVertices("v_IP.IP", ip).nonEmpty) {
        for (v: Vertex <- graph.getVertices("v_IP.IP", ip)) {
          v_IP_Obj = v
        }
      } else {
        v_IP_Obj = graph.addVertex("class:v_IP", Nil: _*)

        v_IP_Obj.setProperty("IP", ip)
        v_IP_Obj.setProperty("IP_LOCATION", "")
        v_IP_Obj.setProperty("FIRST_FOUND_TIME", 0)
        v_IP_Obj.setProperty("LAST_FOUND_TIME", 0)
        v_IP_Obj.setProperty("IP_APPEAR_COUNT", 0)
      }
//      println("e_address_egde:"+v_Fqdn_Obj.getProperty[String]("FQDN_NAME")+"-"+v_IP_Obj.getProperty[String]("IP"))
      //添加或更新边
      for (e: Edge <- v_Fqdn_Obj.getEdges(Direction.OUT)) {
        if (e.getVertex(Direction.IN).getProperty[String]("IP") == ip){
          val cnt = e.getProperty[Long]("COUNT_TOTAL")
          e.setProperty("COUNT_TOTAL",e_Cnt+cnt)
          e.setProperty("LAST_FOUND_TIME",e_Last)
          println("update e_address_egde:"+fqdn+"-"+ip)
          e_Edge_Obj = e
        }
      }
      if (e_Edge_Obj != null){
        val newEdge = graph.addEdge(null, v_Fqdn_Obj, v_IP_Obj, "E_ADDRESS_V_FQDN_TO_V_IP")
        newEdge.setProperty("COUNT_TOTAL",e_Cnt)
        newEdge.setProperty("FIRST_FOUND_TIME",e_First)
        newEdge.setProperty("LAST_FOUND_TIME",e_Last)
        println("insert e_address_egde:"+fqdn+"-"+ip)
      }
    })

    /**
      * 统计e_Visit_v_IP_to_v_FQDN
      */
    e_Visit_v_IP_to_v_FQDN_DF.printSchema()
    e_Visit_v_IP_to_v_FQDN_DF.foreach(row => {
      val fqdn = row.getAs[String]("V_FQDN")
      val ip = row.getAs[String]("V_IP")
      val e_First = row.getAs[Long]("FIRST_FOUND_TIME")
      val e_Last = row.getAs[Long]("LAST_FOUND_TIME")
      val e_Cnt = row.getAs[Long]("COUNT_TOTAL")

      val graph = factory.getNoTx
      var v_Fqdn_Obj: Vertex = null
      var v_IP_Obj: Vertex = null
      var e_Edge_Obj:Edge = null

      import scala.collection.JavaConversions._

      //添加或更新fqdn点
      if (graph.getVertices("v_FQDN.FQDN_NAME", fqdn).nonEmpty) {
        for (v: Vertex <- graph.getVertices("v_FQDN.FQDN_NAME", fqdn)) {
          v_Fqdn_Obj = v
        }
      } else {
        v_Fqdn_Obj = graph.addVertex("class:v_FQDN", Nil: _*)
        v_Fqdn_Obj.setProperty("IP", ip)
        v_Fqdn_Obj.setProperty("FIRST_FOUND_TIME",  0)
        v_Fqdn_Obj.setProperty("LAST_FOUND_TIME",   0)
        v_Fqdn_Obj.setProperty("FQDN_APPEAR_COUNT", 0)
      }

      //添加或更新IP点
      if (graph.getVertices("v_IP.IP", ip).nonEmpty) {
        for (v: Vertex <- graph.getVertices("v_IP.IP", ip)) {
          v_IP_Obj = v
        }
      } else {
        v_IP_Obj = graph.addVertex("class:v_IP", Nil: _*)
        v_IP_Obj.setProperty("FQDN_NAME", fqdn)
        v_IP_Obj.setProperty("IP_LOCATION", "")
        v_IP_Obj.setProperty("FIRST_FOUND_TIME", 0)
        v_IP_Obj.setProperty("LAST_FOUND_TIME", 0)
        v_IP_Obj.setProperty("IP_APPEAR_COUNT", 0)
      }
//      println("e_visit_egde:"+v_Fqdn_Obj.getProperty[String]("FQDN_NAME")+"-"+v_IP_Obj.getProperty[String]("IP"))

      //添加或更新边
      for (e: Edge <- v_IP_Obj.getEdges(Direction.OUT)) {
        if (e.getVertex(Direction.IN).getProperty[String]("FQDN_NAME") == fqdn){
          val cnt = e.getProperty[Long]("COUNT_TOTAL")
          e.setProperty("COUNT_TOTAL",e_Cnt+cnt)
          e.setProperty("LAST_FOUND_TIME",e_Last)
          println("update e_visit_egde:"+fqdn+"-"+ip)
          e_Edge_Obj = e
        }
      }
      if (e_Edge_Obj != null){
        val newEdge = graph.addEdge(null, v_Fqdn_Obj, v_IP_Obj, "E_VISIT_V_IP_TO_V_FQDN")
        newEdge.setProperty("COUNT_TOTAL",e_Cnt)
        newEdge.setProperty("FIRST_FOUND_TIME",e_First)
        newEdge.setProperty("LAST_FOUND_TIME",e_Last)
        println("insert e_visit_egde:"+fqdn+"-"+ip)
      }
      //      graph.commit()
    })




    /*
     v_FQDN_DF.printSchema()
     v_FQDN_DF.coalesce(20).foreach(row => {
       val fqdn = row.getAs[String](0)
       val first = row.getAs[Long](1)
       val last = row.getAs[Long](2)
       val count = row.getAs[Long](3)
       val session = pool.acquire()
       val vertex = session.newVertex("v_FQDN")
       vertex.setProperty("FQDN_NAME",fqdn)
       vertex.setProperty("FIRST_FOUND_TIME", first)
       vertex.setProperty("LAST_FOUND_TIME", last)
       vertex.setProperty("FQDN_APPEAR_COUNT", count)
       vertex
     })



     v_IP_DF.printSchema()
     v_IP_DF.coalesce(20).foreach(row => {
       val ip = row.getAs[String](0)
       val first = row.getAs[Long](2)
       val last = row.getAs[Long](3)
       val count = row.getAs[Long](4)
       val tx: OrientGraph = factory.getTx
       val vertex = tx.addVertex("class:v_FQDN",Nil: _*)
       vertex.setProperties("FQDN_NAME",ip)
       vertex.setProperty("FIRST_FOUND_TIME", first)
       vertex.setProperty("LAST_FOUND_TIME", last)
       vertex.setProperty("FQDN_APPEAR_COUNT", count)
       tx.commit()
     })

     e_Address_v_FQDN_to_v_IP_DF.printSchema()
     e_Address_v_FQDN_to_v_IP_DF.foreach(row => {
       val fqdn = row.getAs[String](0)
       val ip = row.getAs[String](2)
       val first = row.getAs[Long](3)
       val last = row.getAs[Long](4)
       val count = row.getAs[Long](5)
       val session = pool.acquire()
       val tx: OrientGraph = factory.getTx
       tx.getFeatures.supportsVertexProperties
       val vertex: OrientVertex = tx.getVertex()
       tx.addEdge(null,vertex,vertex,"")
     })
     */
  }

}
