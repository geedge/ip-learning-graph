package cn.ac.iie.spark

import cn.ac.iie.config.ApplicationConfig
import cn.ac.iie.dao.BaseClickhouseData
import cn.ac.iie.spark.partition.CustomPartitioner
import cn.ac.iie.spark.rdd.{ArangoRdd, ReadOptions}
import cn.ac.iie.utils.SparkSessionUtil
import com.arangodb.entity.BaseDocument
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{collect_list, max, min}
import org.apache.spark.storage.StorageLevel

object RDDTest {
  def main(args: Array[String]): Unit = {

    val sparkContext = SparkSessionUtil.spark.sparkContext

    println(sparkContext.getConf.get("arangodb.hosts"))

    //    val options = ReadOptions("iplearn_media_domain").copy(collection = "R_LOCATE_FQDN2IP")
    val options = ReadOptions(ApplicationConfig.ARANGODB_DB_NAME)

    val ipOptions = options.copy(collection = "IP")

    val rdd: ArangoRdd[BaseDocument] = ArangoSpark.load[BaseDocument](sparkContext,"IP",options)

    println(rdd.count())
    println(rdd.getNumPartitions)

    val ipRDD = mergeVertexIp()
    val value: RDD[(String, (Option[BaseDocument], Option[Row]))] = rdd.map(doc => {
      (doc.getKey, doc)
    }).fullOuterJoin(ipRDD)
    value.foreach((row: (String, (Option[BaseDocument], Option[Row]))) => {
      val value = row._2._2
      val str: String = value match {
        case Some(r) => r.getAs[String]("IP")
//        case None => null
        case _ => null
      }
      println(str)
    })

    /*
    val value: RDD[BaseDocument] = rdd.filter(doc => doc.getAttribute("CLIENT_SESSION_COUNT").asInstanceOf[Long] > 100).map(doc => {
      doc.addAttribute("abc", 1)
      doc
    })
    value.map(doc => {(doc.getKey,doc)})
    value.persist(StorageLevel.MEMORY_AND_DISK)
    value.foreach(fqdnRow => println(fqdnRow.toString))
    println(value.count())
    */

    SparkSessionUtil.spark.close()
    System.exit(0)

  }

  def mergeVertexIp(): RDD[(String,Row)]={
    val vertexIpDf = BaseClickhouseData.getVertexIpDf
    val frame = vertexIpDf.groupBy("IP").agg(
      min("FIRST_FOUND_TIME").alias("FIRST_FOUND_TIME"),
      max("LAST_FOUND_TIME").alias("LAST_FOUND_TIME"),
      collect_list("SESSION_COUNT").alias("SESSION_COUNT_LIST"),
      collect_list("BYTES_SUM").alias("BYTES_SUM_LIST"),
      collect_list("ip_type").alias("ip_type_list")
    )
    val values = frame.rdd.map(row => (row.getAs[String]("IP"), row))
      .partitionBy(new CustomPartitioner(ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS))
    values
  }

}
