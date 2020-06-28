package cn.ac.iie.test

import cn.ac.iie.dao.{BaseMediaDataLoad, UpdateArangoGraphByDF}
import cn.ac.iie.utils.{ConfigUtils, InitArangoDBPool}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object ArangoDbTestMemoryGroupBy {

  private val logger: Logger = LoggerFactory.getLogger(ArangoDbTestMemoryGroupBy.getClass)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(ConfigUtils.SPARK_APP_NAME)
      .config("spark.serializer", ConfigUtils.SPARK_SERIALIZER)
      .config("spark.network.timeout", ConfigUtils.SPARK_NETWORK_TIMEOUT)
      .config("spark.sql.shuffle.partitions", ConfigUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.executor.memory", ConfigUtils.SPARK_EXECUTOR_MEMORY)
      .master(ConfigUtils.MASTER)
      .getOrCreate()
    logger.warn("sparkession获取成功！！！")

    BaseMediaDataLoad.loadMediaDate(spark)
    val v_FQDN_DF = BaseMediaDataLoad.getFQDNVertexFromMedia(spark)
    val v_IP_DF = BaseMediaDataLoad.getIPVertexFromMedia(spark)
    val e_Address_v_FQDN_to_v_IP_DF = BaseMediaDataLoad.getFQDNAddressIPEdgeFromMedia(spark)
    val e_Visit_v_IP_to_v_FQDN_DF= BaseMediaDataLoad.getIPVisitFQDNEdgeFromMedia(spark)

    UpdateArangoGraphByDF.updateFQDNVertex(v_FQDN_DF,spark)
    UpdateArangoGraphByDF.updateIPVertex(v_IP_DF,spark)
    UpdateArangoGraphByDF.updateFQDNAddressIPEdge(e_Address_v_FQDN_to_v_IP_DF,spark)
    UpdateArangoGraphByDF.updateIPVisitFQDNEdge(e_Visit_v_IP_to_v_FQDN_DF,spark)

    InitArangoDBPool.arangoDB.shutdown()
    spark.close()

  }

}
