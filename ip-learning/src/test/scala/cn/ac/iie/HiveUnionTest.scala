package cn.ac.iie

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveUnionTest {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.network.timeout", "300s")
      .config("spark.sql.shuffle.partitions", 50)
      .master("local[*]")

      /*
      .config("spark.executor.memory", "30g")
      .config("spark.driver.host", "192.168.41.79")
      .config("spark.jars", "D:\\GITREPO\\ip-learning\\target\\ip-learning-1.0-SNAPSHOT-jar-with-dependencies.jar")
      .master("spark://192.168.40.119:7077")
      */

      .getOrCreate()

    val sql =
      """
        |(SELECT  s1_s_ip AS IP,  s1_s_location_region AS location,recv_time FROM av_miner.media_expire_patch limit 100
        |UNION ALL
        |SELECT  s1_d_ip AS IP,  s1_d_location_region AS location,  recv_time FROM av_miner.media_expire_patch limit 100)
      """.stripMargin

    val mediaDataFrame: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:clickhouse://192.168.40.193:8123")
      .option("dbtable","(select * from av_miner.media_expire_patch limit 100) as media_expire_patch")
//      .option("dbtable", "av_miner.media_expire_patch")
      //      .option("dbtable", sql + " as a")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("user", "default")
      .option("password", "111111")
      .option("numPartitions", "40")
      .option("fetchsize", "1000000")
      .load()
    mediaDataFrame.printSchema()
    mediaDataFrame.createOrReplaceGlobalTempView("media_expire_patch")
    val frame = spark.sql(
      """
         select IP,location,MIN(recv_time) AS FIRST_FOUND_TIME, MAX(recv_time) AS LAST_FOUND_TIME, COUNT(*) AS COUNT_TOTAL from (
          (SELECT  s1_s_ip AS IP,  s1_s_location_region AS location,recv_time FROM global_temp.media_expire_patch limit 100)
          UNION ALL
          (SELECT  s1_d_ip AS IP,  s1_d_location_region AS location,  recv_time FROM global_temp.media_expire_patch limit 100)
          ) group by IP,location
      """.stripMargin)

    val e_Address_v_FQDN_to_v_IP_DF = spark.sql(
      """
        |SELECT
        |	media_domain AS V_FQDN,
        |	media_type,
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
        |	media_domain,
        |	media_type
      """.stripMargin)
    e_Address_v_FQDN_to_v_IP_DF.printSchema()
    e_Address_v_FQDN_to_v_IP_DF.show(200)
//    mediaDataFrame.show(20)
  }

}
