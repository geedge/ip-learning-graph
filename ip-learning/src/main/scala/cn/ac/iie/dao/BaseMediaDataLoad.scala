package cn.ac.iie.dao

import cn.ac.iie.test.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

object BaseMediaDataLoad {

  def loadMediaDate(spark: SparkSession): Unit = {
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
  }

  def getFQDNVertexFromMedia(spark: SparkSession): DataFrame = {
    val v_FQDN_DF = spark.sql(
      """
        |SELECT
        |	media_domain AS new_fqdn_name,
        |	MIN( recv_time ) AS new_fqdn_first_found_time,
        |	MAX( recv_time ) AS new_fqdn_last_found_time,
        |	COUNT( * ) AS new_fqdn_count_total
        |FROM
        |	global_temp.media_expire_patch
        |WHERE
        |	media_domain != ''
        |GROUP BY
        |	media_domain
      """.stripMargin
    )
    v_FQDN_DF
  }

  def getIPVertexFromMedia(spark: SparkSession): DataFrame = {
    val s_IP_DF = spark.sql(
      """
        select
        	s1_s_ip as new_ip,
        	s1_s_location_region as new_location,
        	MIN( recv_time ) AS new_ip_first_found_time,
        	MAX( recv_time ) AS new_ip_last_found_time,
        	COUNT( * ) AS new_ip_count_total
         from global_temp.media_expire_patch
         GROUP BY
        	s1_s_ip,
        	s1_s_location_region
      """.stripMargin)
    val d_IP_DF = spark.sql(
      """
        select
        	s1_d_ip as new_ip,
        	s1_d_location_region as new_location,
|        	MIN( recv_time ) AS new_ip_first_found_time,
         	MAX( recv_time ) AS new_ip_last_found_time,
         	COUNT( * ) AS new_ip_count_total
         from global_temp.media_expire_patch
         GROUP BY
        	s1_d_ip,
        	s1_d_location_region
      """.stripMargin)
    import org.apache.spark.sql.functions._
    val v_IP_DF = s_IP_DF.union(d_IP_DF).groupBy("new_ip", "new_location").agg(
      min("new_ip_first_found_time").as("new_ip_first_found_time"),
      max("new_ip_last_found_time").as("new_ip_last_found_time"),
      count("new_ip").as("new_ip_count_total")
    )
    v_IP_DF
  }

  def getFQDNAddressIPEdgeFromMedia(spark: SparkSession): DataFrame = {
    val e_Address_v_FQDN_to_v_IP_DF = spark.sql(
      """
        |SELECT
        |	media_domain AS new_fqdn,
        |	s1_d_ip AS new_ip,
        |	MIN( recv_time ) AS new_first_found_time,
        |	MAX( recv_time ) AS new_last_found_time,
        |	COUNT( * ) AS new_count_total,
        | CONCAT_WS('-',media_domain,s1_d_ip) AS new_key
        |FROM
        |	global_temp.media_expire_patch
        |WHERE
        |	( media_domain != '' )
        |	AND ( s1_d_ip != '' )
        |GROUP BY
        |	s1_d_ip,
        |	media_domain
      """.stripMargin)
    e_Address_v_FQDN_to_v_IP_DF
  }

  def getIPVisitFQDNEdgeFromMedia(spark: SparkSession): DataFrame = {
    val e_Visit_v_IP_to_v_FQDN_DF = spark.sql(
      """
        |SELECT
        |	s1_s_ip AS new_ip,
        |	media_domain AS new_fqdn,
        |	MIN( recv_time ) AS new_first_found_time,
        |	MAX( recv_time ) AS new_last_found_time,
        |	COUNT( * ) AS new_count_total,
        | CONCAT_WS('-',s1_s_ip,media_domain) as new_key
        |FROM
        |	global_temp.media_expire_patch
        |WHERE
        |	( s1_s_ip != '' )
        |	AND ( media_domain != '' )
        |GROUP BY
        |	s1_s_ip,
        |	media_domain
      """.stripMargin)
    e_Visit_v_IP_to_v_FQDN_DF
  }


}
