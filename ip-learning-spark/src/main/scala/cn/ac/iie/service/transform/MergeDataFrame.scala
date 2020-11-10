package cn.ac.iie.service.transform

import java.util.regex.Pattern

import cn.ac.iie.config.ApplicationConfig
import cn.ac.iie.dao.BaseClickhouseData
import cn.ac.iie.spark.ArangoSpark
import cn.ac.iie.spark.partition.CustomPartitioner
import cn.ac.iie.spark.rdd.ReadOptions
import com.arangodb.entity.{BaseDocument, BaseEdgeDocument}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import cn.ac.iie.utils.SparkSessionUtil._

object MergeDataFrame {
  private val LOG = LoggerFactory.getLogger(MergeDataFrame.getClass)
  private val pattern = Pattern.compile("^[\\d]*$")
  private val options = ReadOptions(ApplicationConfig.ARANGODB_DB_NAME)

  def mergeVertexFqdn(): RDD[(String, (Option[BaseDocument], Option[Row]))] ={
    val fqdnAccmu = getLongAccumulator("FQDN Accumulator")
    val fqdnRddRow = BaseClickhouseData.getVertexFqdnDf
      .rdd.filter(row => isDomain(row.getAs[String](0))).map(row => {
      fqdnAccmu.add(1)
      (row.getAs[String]("FQDN"), row)
    }).partitionBy(new CustomPartitioner(ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS))
    fqdnRddRow.cache()
    val fqdnRddDoc = ArangoSpark.load[BaseDocument](sparkContext,"FQDN",options)

    fqdnRddDoc.map(doc => (doc.getKey, doc)).fullOuterJoin(fqdnRddRow)
  }

  def mergeVertexIp(): RDD[(String, (Option[BaseDocument], Option[Row]))]={
    val ipAccum = getLongAccumulator("IP Accumulator")
    val vertexIpDf = BaseClickhouseData.getVertexIpDf
    val frame = vertexIpDf.groupBy("IP").agg(
      min("FIRST_FOUND_TIME").alias("FIRST_FOUND_TIME"),
      max("LAST_FOUND_TIME").alias("LAST_FOUND_TIME"),
      collect_list("SESSION_COUNT").alias("SESSION_COUNT_LIST"),
      collect_list("BYTES_SUM").alias("BYTES_SUM_LIST"),
      collect_list("ip_type").alias("ip_type_list"),
      last("common_link_info").alias("common_link_info")
    )
    val ipRddRow = frame.rdd.map(row => {
      ipAccum.add(1)
      (row.getAs[String]("IP"), row)
    }).partitionBy(new CustomPartitioner(ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS))
    val ipRddDoc = ArangoSpark.load[BaseDocument](sparkContext,"IP",options)

    ipRddDoc.map(doc => (doc.getKey, doc)).fullOuterJoin(ipRddRow)

  }

  def mergeRelationFqdnLocateIp(): RDD[(String, (Option[BaseEdgeDocument], Option[Row]))] ={
    val fqdnLocIpAccum = getLongAccumulator("R_LOCATE_FQDN2IP Accumulator")
    val frame = BaseClickhouseData.getRelationFqdnLocateIpDf.filter(row => isDomain(row.getAs[String]("FQDN")))
      .groupBy("FQDN", "common_server_ip")
      .agg(
        min("FIRST_FOUND_TIME").alias("FIRST_FOUND_TIME"),
        max("LAST_FOUND_TIME").alias("LAST_FOUND_TIME"),
        collect_list("COUNT_TOTAL").alias("COUNT_TOTAL_LIST"),
        collect_list("schema_type").alias("schema_type_list"),
        collect_set("DIST_CIP_RECENT").alias("DIST_CIP_RECENT")
      )
    val fqdnLocIpRddRow = frame.rdd.map(row => {
      val fqdn = row.getAs[String]("FQDN")
      val serverIp = row.getAs[String]("common_server_ip")
      val key = fqdn.concat("-" + serverIp)
      fqdnLocIpAccum.add(1)
      (key, row)
    }).partitionBy(new CustomPartitioner(ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS))
    val fqdnLocIpRddDoc = ArangoSpark.load[BaseEdgeDocument](sparkContext,"R_LOCATE_FQDN2IP",options)

    fqdnLocIpRddDoc.map(doc => (doc.getKey, doc)).fullOuterJoin(fqdnLocIpRddRow)

  }

  def mergeRelationSubidLocateIp(): RDD[(String, (Option[BaseEdgeDocument], Option[Row]))] ={
    val subidLocIpAccum = getLongAccumulator("R_LOCATE_SUBSCRIBER2IP Accumulator")
    val subidLocIpRddRow = BaseClickhouseData.getRelationSubidLocateIpDf
      .repartition(ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS)
      .rdd.map(row => {
      val commonSubscriberId = row.getAs[String]("common_subscriber_id")
      val ip = row.getAs[String]("radius_framed_ip")
      val key = commonSubscriberId.concat("-" + ip)
      subidLocIpAccum.add(1)
      (key, row)
    }).partitionBy(new CustomPartitioner(ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS))
    val subidLocIpRddDoc = ArangoSpark.load[BaseEdgeDocument](sparkContext,"R_LOCATE_SUBSCRIBER2IP",options)

    subidLocIpRddDoc.map(doc => (doc.getKey, doc)).fullOuterJoin(subidLocIpRddRow)
  }

  def mergeVertexSubid(): RDD[(String, (Option[BaseDocument], Option[Row]))] ={
    val subidAccum = getLongAccumulator("SUBSCRIBER Accumulator")
    val subidRddRow = BaseClickhouseData.getVertexSubidDf
      .repartition(ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS)
      .rdd.map(row => {
      val commonSubscriberId = row.getAs[String]("common_subscriber_id")
      subidAccum.add(1)
      (commonSubscriberId, row)
    }).partitionBy(new CustomPartitioner(ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS))

    val subidRddDoc = ArangoSpark.load[BaseDocument](sparkContext,"SUBSCRIBER",options)

    subidRddDoc.map(doc => (doc.getKey, doc)).fullOuterJoin(subidRddRow)

  }

  def mergeVertexFrameIp: RDD[Row] ={
    val framedIpAccum = getLongAccumulator("framed ip Accumulator")
    val values = BaseClickhouseData.getVertexFramedIpDf
      .repartition(ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS)
      .rdd.map(row => {
      val ip = row.getAs[String]("radius_framed_ip")
      framedIpAccum.add(1)
      (ip, row)
    }).partitionBy(new CustomPartitioner(ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS)).values
    values
  }

  private def isDomain(fqdn: String): Boolean = {
    try {
      if (fqdn == null || fqdn.length == 0) {
        return false
      }

      val fqdnArr = fqdn.split(":")(0).split("\\.")

      if (fqdnArr.length != 4){
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
