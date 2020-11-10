package cn.ac.iie.dao

import cn.ac.iie.config.ApplicationConfig
import cn.ac.iie.utils.SparkSessionUtil.spark
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object BaseClickhouseData {
  private val LOG = LoggerFactory.getLogger(BaseClickhouseData.getClass)

  val currentHour: Long = System.currentTimeMillis / (60 * 60 * 1000) * 60 * 60
  private val timeLimit: (Long, Long) = getTimeLimit

  private def initClickhouseData(sql:String): DataFrame  ={

    val dataFrame: DataFrame = spark.read.format("jdbc")
      .option("url", ApplicationConfig.SPARK_READ_CLICKHOUSE_URL)
      .option("dbtable", sql)
      .option("driver", ApplicationConfig.SPARK_READ_CLICKHOUSE_DRIVER)
      .option("user", ApplicationConfig.SPARK_READ_CLICKHOUSE_USER)
      .option("password", ApplicationConfig.SPARK_READ_CLICKHOUSE_PASSWORD)
      .option("numPartitions", ApplicationConfig.NUMPARTITIONS)
      .option("partitionColumn", ApplicationConfig.SPARK_READ_CLICKHOUSE_PARTITIONCOLUMN)
      .option("lowerBound", timeLimit._2)
      .option("upperBound", timeLimit._1)
      .option("fetchsize", ApplicationConfig.SPARK_READ_CLICKHOUSE_FETCHSIZE)
      .option("socket_timeout",ApplicationConfig.CLICKHOUSE_SOCKET_TIMEOUT)
      .load()
    dataFrame.printSchema()
    dataFrame.createOrReplaceGlobalTempView("dbtable")

    dataFrame
  }

  def loadConnectionDataFromCk(): Unit ={
    val where = "common_recv_time >= " + timeLimit._2 + " AND common_recv_time < " + timeLimit._1
    val sql =
      s"""
        |(SELECT
        | ssl_sni,http_host,common_client_ip,common_server_ip,common_recv_time,common_c2s_byte_num,common_s2c_byte_num,common_schema_type
        |FROM
        | connection_record_log
        |WHERE $where) as dbtable
      """.stripMargin

    LOG.warn(sql)
    initClickhouseData(sql)
  }

  private def loadRadiusDataFromCk(): Unit ={
    val where =
      s"""
        | common_recv_time >= ${timeLimit._2}
        | AND common_recv_time < ${timeLimit._1}
        | AND common_subscriber_id != ''
        | AND radius_framed_ip != ''
        | AND radius_packet_type = 4
        | AND radius_acct_status_type = 1
      """.stripMargin
    val sql =
      s"""
        |(SELECT
        | common_subscriber_id,radius_framed_ip,common_recv_time
        |FROM
        | tsg_galaxy_v3.radius_record_log
        |WHERE
        | $where) as dbtable
      """.stripMargin
    LOG.warn(sql)
    initClickhouseData(sql)
  }

  /*
  def getVertexIpDf: DataFrame ={
    loadConnectionDataFromCk()
    val sql =
      """
        |SELECT
        | *
        |FROM
        | (
        |  (
        |   SELECT
        |    common_client_ip AS IP,
        |    MIN(common_recv_time) AS FIRST_FOUND_TIME,
        |    MAX(common_recv_time) AS LAST_FOUND_TIME,
        |    count(*) as SESSION_COUNT,
        |    sum(common_c2s_byte_num) as BYTES_SUM,
        |    'client' as ip_type
        |   FROM
        |    global_temp.dbtable
        |   GROUP BY
        |    IP
        |  )
        | UNION ALL
        |  (
        |   SELECT
        |    common_server_ip AS IP,
        |    MIN(common_recv_time) AS FIRST_FOUND_TIME,
        |    MAX(common_recv_time) AS LAST_FOUND_TIME,
        |    count(*) as SESSION_COUNT,
        |    sum(common_s2c_byte_num) as BYTES_SUM,
        |    'server' as ip_type
        |   FROM
        |    global_temp.dbtable
        |   GROUP BY
        |    IP
        |  )
        | )
      """.stripMargin
    LOG.warn(sql)
    val vertexIpDf = spark.sql(sql)
    vertexIpDf.printSchema()
    vertexIpDf
  }

  def getRelationFqdnLocateIpDf: DataFrame ={
    loadConnectionDataFromCk()
    val sslSql =
      """
        |SELECT
        | ssl_sni AS FQDN,
        | common_server_ip,
        | MAX(common_recv_time) AS LAST_FOUND_TIME,
        | MIN(common_recv_time) AS FIRST_FOUND_TIME,
        | COUNT(*) AS COUNT_TOTAL,
        | collect_set(common_client_ip) AS DIST_CIP_RECENT,
        | 'TLS' AS  schema_type
        |FROM
        | global_temp.dbtable
        |WHERE
        | common_schema_type = 'SSL'
        |GROUP BY
        | ssl_sni,common_server_ip
      """.stripMargin

    val httpSql =
      """
        |SELECT
        | http_host AS FQDN,
        | common_server_ip,
        | MAX(common_recv_time) AS LAST_FOUND_TIME,
        | MIN(common_recv_time) AS FIRST_FOUND_TIME,
        | COUNT(*) AS COUNT_TOTAL,
        | collect_set(common_client_ip) AS DIST_CIP_RECENT,
        | 'HTTP' AS schema_type
        |FROM
        | global_temp.dbtable
        |WHERE
        | common_schema_type = 'HTTP'
        |GROUP BY
        | http_host,common_server_ip
      """.stripMargin
    val sql = s"SELECT * FROM (($sslSql) UNION ALL ($httpSql)) WHERE FQDN != ''"

    LOG.warn(sql)
    val relationFqdnLocateIpDf = spark.sql(sql)
    relationFqdnLocateIpDf.printSchema()
    relationFqdnLocateIpDf
  }
  */

  def getVertexFqdnDf: DataFrame ={
    val sql =
      """
        |(SELECT
        | FQDN,MAX( LAST_FOUND_TIME ) AS LAST_FOUND_TIME,MIN( FIRST_FOUND_TIME ) AS FIRST_FOUND_TIME
        |FROM
        | ((SELECT
        |    ssl_sni AS FQDN,MAX( common_recv_time ) AS LAST_FOUND_TIME,MIN( common_recv_time ) AS FIRST_FOUND_TIME
        |   FROM tsg_galaxy_v3.connection_record_log
        |   WHERE common_schema_type = 'SSL' GROUP BY ssl_sni
        |  )UNION ALL
        |  (SELECT
        |    http_host AS FQDN,MAX( common_recv_time ) AS LAST_FOUND_TIME,MIN( common_recv_time ) AS FIRST_FOUND_TIME
        |   FROM tsg_galaxy_v3.connection_record_log
        |   WHERE common_schema_type = 'HTTP' GROUP BY http_host))
        |GROUP BY FQDN HAVING FQDN != '') as dbtable
      """.stripMargin
    LOG.warn(sql)
    val frame = initClickhouseData(sql)
    frame.printSchema()
    frame
  }

  def getVertexIpDf: DataFrame ={
    val where = "common_recv_time >= " + timeLimit._2 + " AND common_recv_time < " + timeLimit._1
    val sql =
      s"""
         |(SELECT * FROM
         |((SELECT common_client_ip AS IP,MIN(common_recv_time) AS FIRST_FOUND_TIME,
         |MAX(common_recv_time) AS LAST_FOUND_TIME,
         |count(*) as SESSION_COUNT,
         |SUM(common_c2s_byte_num+common_s2c_byte_num) as BYTES_SUM,
         |groupUniqArray(2)(common_link_info_c2s)[2] as common_link_info,
         |'client' as ip_type
         |FROM tsg_galaxy_v3.connection_record_log
         |where $where
         |group by common_client_ip)
         |UNION ALL
         |(SELECT common_server_ip AS IP,
         |MIN(common_recv_time) AS FIRST_FOUND_TIME,
         |MAX(common_recv_time) AS LAST_FOUND_TIME,
         |count(*) as SESSION_COUNT,
         |SUM(common_c2s_byte_num+common_s2c_byte_num) as BYTES_SUM,
         |groupUniqArray(2)(common_link_info_s2c)[2] as common_link_info,
         |'server' as ip_type
         |FROM tsg_galaxy_v3.connection_record_log
         |where $where
         |group by common_server_ip))) as dbtable
      """.stripMargin
    LOG.warn(sql)
    val frame = initClickhouseData(sql)
    frame.printSchema()
    frame
  }


  def getRelationFqdnLocateIpDf: DataFrame ={
    val where = "common_recv_time >= " + timeLimit._2 + " AND common_recv_time < " + timeLimit._1
    val sql =
      s"""
         |(SELECT * FROM
         |((SELECT ssl_sni AS FQDN,common_server_ip,MAX(common_recv_time) AS LAST_FOUND_TIME,MIN(common_recv_time) AS FIRST_FOUND_TIME,COUNT(*) AS COUNT_TOTAL,
         |toString(groupUniqArray(${ApplicationConfig.DISTINCT_CLIENT_IP_NUM})(common_client_ip)) AS DIST_CIP_RECENT,'TLS' AS  schema_type
         |FROM tsg_galaxy_v3.connection_record_log
         |WHERE $where and common_schema_type = 'SSL' GROUP BY ssl_sni,common_server_ip)
         |UNION ALL
         |(SELECT http_host AS FQDN,common_server_ip,MAX(common_recv_time) AS LAST_FOUND_TIME,MIN(common_recv_time) AS FIRST_FOUND_TIME,COUNT(*) AS COUNT_TOTAL,
         |toString(groupUniqArray(${ApplicationConfig.DISTINCT_CLIENT_IP_NUM})(common_client_ip)) AS DIST_CIP_RECENT,'HTTP' AS schema_type
         |FROM tsg_galaxy_v3.connection_record_log
         |WHERE $where and common_schema_type = 'HTTP' GROUP BY http_host,common_server_ip))
         |WHERE FQDN != '') as dbtable
      """.stripMargin
    LOG.warn(sql)
    val frame = initClickhouseData(sql)
    frame.printSchema()
    frame
  }

  def getRelationSubidLocateIpDf: DataFrame ={
    val where =
      s"""
         | common_recv_time >= ${timeLimit._2}
         | AND common_recv_time < ${timeLimit._1}
         | AND common_subscriber_id != ''
         | AND radius_framed_ip != ''
      """.stripMargin
    val sql =
      s"""
         |(
         |SELECT common_subscriber_id,radius_framed_ip,MAX(common_recv_time) as LAST_FOUND_TIME,MIN(common_recv_time) as FIRST_FOUND_TIME
         |FROM radius_record_log
         |WHERE $where GROUP BY common_subscriber_id,radius_framed_ip
         |) as dbtable
      """.stripMargin
    LOG.warn(sql)
    val frame = initClickhouseData(sql)
    frame.printSchema()
    frame
  }

  def getVertexSubidDf: DataFrame ={
    val where =
      s"""
         | common_recv_time >= ${timeLimit._2}
         | AND common_recv_time < ${timeLimit._1}
         | AND common_subscriber_id != ''
         | AND radius_framed_ip != ''
      """.stripMargin
    val sql =
      s"""
         |(
         |SELECT common_subscriber_id,MAX(common_recv_time) as LAST_FOUND_TIME,MIN(common_recv_time) as FIRST_FOUND_TIME FROM radius_record_log
         |WHERE $where GROUP BY common_subscriber_id
         |)as dbtable
      """.stripMargin
    LOG.warn(sql)
    val frame = initClickhouseData(sql)
    frame.printSchema()
    frame
  }

  def getVertexFramedIpDf: DataFrame ={
    val where =
      s"""
         | common_recv_time >= ${timeLimit._2}
         | AND common_recv_time < ${timeLimit._1}
         | AND common_subscriber_id != ''
         | AND radius_framed_ip != ''
      """.stripMargin
    val sql =
      s"""
         |(
         |SELECT DISTINCT radius_framed_ip,common_recv_time as LAST_FOUND_TIME FROM radius_record_log WHERE $where
         |)as dbtable
      """.stripMargin
    LOG.warn(sql)
    val frame = initClickhouseData(sql)
    frame.printSchema()
    frame
  }


  private def getTimeLimit: (Long,Long) ={
    var maxTime = 0L
    var minTime = 0L
    ApplicationConfig.CLICKHOUSE_TIME_LIMIT_TYPE match {
      case 0 =>
        maxTime = currentHour
        minTime = maxTime - ApplicationConfig.UPDATE_INTERVAL
      case 1 =>
        maxTime = ApplicationConfig.READ_CLICKHOUSE_MAX_TIME
        minTime = ApplicationConfig.READ_CLICKHOUSE_MIN_TIME
      case _ =>
    }
    (maxTime, minTime)
  }

}
