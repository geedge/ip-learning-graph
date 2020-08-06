package cn.ac.iie.service.transform

import java.util.regex.Pattern

import cn.ac.iie.config.ApplicationConfig
import cn.ac.iie.dao.BaseClickhouseData
import cn.ac.iie.spark.partition.CustomPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object MergeDataFrame {
  private val LOG = LoggerFactory.getLogger(MergeDataFrame.getClass)
  private val pattern = Pattern.compile("^[\\d]*$")

  def mergeVertexFqdn(): RDD[Row] ={
    BaseClickhouseData.getVertexFqdnDf
      .rdd.filter(row => isDomain(row.getAs[String](0))).map(row => (row.get(0),row))
      .partitionBy(new CustomPartitioner(ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS)).values
  }

  def mergeVertexIp(): RDD[Row]={
    val vertexIpDf = BaseClickhouseData.getVertexIpDf
    val frame = vertexIpDf.groupBy("IP").agg(
      min("FIRST_FOUND_TIME").alias("FIRST_FOUND_TIME"),
      max("LAST_FOUND_TIME").alias("LAST_FOUND_TIME"),
      collect_list("SESSION_COUNT").alias("SESSION_COUNT_LIST"),
      collect_list("BYTES_SUM").alias("BYTES_SUM_LIST"),
      collect_list("ip_type").alias("ip_type_list")
    )
    val values = frame.rdd.map(row => (row.get(0), row))
      .partitionBy(new CustomPartitioner(ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS)).values
    values
  }

  def mergeRelationFqdnLocateIp(): RDD[Row] ={
    val frame = BaseClickhouseData.getRelationFqdnLocateIpDf.filter(row => isDomain(row.getAs[String]("FQDN")))
      .groupBy("FQDN", "common_server_ip")
      .agg(
        min("FIRST_FOUND_TIME").alias("FIRST_FOUND_TIME"),
        max("LAST_FOUND_TIME").alias("LAST_FOUND_TIME"),
        collect_list("COUNT_TOTAL").alias("COUNT_TOTAL_LIST"),
        collect_list("schema_type").alias("schema_type_list"),
        collect_set("DIST_CIP_RECENT").alias("DIST_CIP_RECENT")
      )
    frame.rdd.map(row => {
      val fqdn = row.getAs[String]("FQDN")
      val serverIp = row.getAs[String]("common_server_ip")
      val key = fqdn.concat("-"+serverIp)
      (key,row)
    }).partitionBy(new CustomPartitioner(ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS)).values

  }

  private def isDomain(fqdn: String): Boolean = {
    try {
      if (fqdn == null || fqdn.length == 0) {
        return false
      }
      if (fqdn.contains(":")) {
        val s = fqdn.split(":")(0)
        if (s.contains(":")){
          return false
        }
      }
      val fqdnArr = fqdn.split("\\.")
      if (fqdnArr.length < 4 || fqdnArr.length > 4){
        return true
      }
      for (f <- fqdnArr) {
        if (pattern.matcher(f).matches) {
          val i = f.toLong
          if (i < 0 || i > 255) {
            return true
          }
        } else {
          return true
        }
      }
    } catch {
      case e: Exception =>
        LOG.error("解析域名 " + fqdn + " 失败：\n" + e.toString)
    }
    false
  }

}
