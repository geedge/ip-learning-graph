package cn.ac.iie.test

import cn.ac.iie.dao.{BaseMediaDataLoad, UpdateArangoGraphByArangoSpark}
import cn.ac.iie.pojo.{BaseEdgeIPVisitFqdn, BaseEgdeFqdnAddressIP, BaseVertexFqdn, BaseVertexIP}
import cn.ac.iie.utils.ConfigUtils
import com.arangodb.spark.rdd.ArangoRDD
import com.arangodb.spark.{ArangoSpark, ReadOptions}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ArangoDBSparkTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(ConfigUtils.SPARK_APP_NAME)
      .config("spark.serializer", ConfigUtils.SPARK_SERIALIZER)
      .config("spark.network.timeout", ConfigUtils.SPARK_NETWORK_TIMEOUT)
      .config("spark.sql.shuffle.partitions", ConfigUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.executor.memory", ConfigUtils.SPARK_EXECUTOR_MEMORY)
      .config("arangodb.hosts", "192.168.40.127:8529")
      .config("arangodb.user", ConfigUtils.ARANGODB_USER)
      .config("arangodb.password", ConfigUtils.ARANGODB_PASSWORD)
      .config("arangodb.maxConnections",ConfigUtils.MAXPOOLSIZE)
      .master(ConfigUtils.MASTER)
      .getOrCreate()

    BaseMediaDataLoad.loadMediaDate(spark)
    val v_FQDN_DF = BaseMediaDataLoad.getFQDNVertexFromMedia(spark)
    val v_IP_DF = BaseMediaDataLoad.getIPVertexFromMedia(spark)
    val e_Address_v_FQDN_to_v_IP_DF = BaseMediaDataLoad.getFQDNAddressIPEdgeFromMedia(spark)
    val e_Visit_v_IP_to_v_FQDN_DF= BaseMediaDataLoad.getIPVisitFQDNEdgeFromMedia(spark)

    val v_FQDN_Cursor_Rdd: ArangoRDD[BaseVertexFqdn] = ArangoSpark.load[BaseVertexFqdn](spark.sparkContext, "V_FQDN", ReadOptions(ConfigUtils.ARANGODB_DB_NAME))
    val v_FQDN_Cursor_DF: DataFrame = spark.createDataFrame(v_FQDN_Cursor_Rdd,classOf[BaseVertexFqdn])

    val v_IP_Cursor_Rdd: ArangoRDD[BaseVertexIP] = ArangoSpark.load[BaseVertexIP](spark.sparkContext, "V_IP", ReadOptions(ConfigUtils.ARANGODB_DB_NAME))
    val v_IP_Cursor_DF: DataFrame = spark.createDataFrame(v_IP_Cursor_Rdd,classOf[BaseVertexIP])

    val e_Fqdn_Address_IP_Cursor_Rdd: ArangoRDD[BaseEgdeFqdnAddressIP] = ArangoSpark.load[BaseEgdeFqdnAddressIP](spark.sparkContext, "E_ADDRESS_V_FQDN_TO_V_IP", ReadOptions(ConfigUtils.ARANGODB_DB_NAME))
    val e_Fqdn_Address_IP_Cursor_DF: DataFrame = spark.createDataFrame(e_Fqdn_Address_IP_Cursor_Rdd,classOf[BaseEgdeFqdnAddressIP])

    val e_IP_Visit_FQDN_Cursor_Rdd: ArangoRDD[BaseEdgeIPVisitFqdn] = ArangoSpark.load[BaseEdgeIPVisitFqdn](spark.sparkContext, "E_VISIT_V_IP_TO_V_FQDN", ReadOptions(ConfigUtils.ARANGODB_DB_NAME))
    val e_IP_Visit_FQDN_Cursor_DF: DataFrame = spark.createDataFrame(e_IP_Visit_FQDN_Cursor_Rdd,classOf[BaseEdgeIPVisitFqdn])

    UpdateArangoGraphByArangoSpark.updateFQDNVertex(v_FQDN_DF,v_FQDN_Cursor_DF)
    UpdateArangoGraphByArangoSpark.updateIPVertex(v_IP_DF,v_IP_Cursor_DF)
    UpdateArangoGraphByArangoSpark.updateFQDNAddressIPEdge(e_Address_v_FQDN_to_v_IP_DF,e_Fqdn_Address_IP_Cursor_DF)
    UpdateArangoGraphByArangoSpark.updateIPVisitFQDNEdge(e_Visit_v_IP_to_v_FQDN_DF,e_IP_Visit_FQDN_Cursor_DF)
    spark.close()
    System.exit(1)
  }

}
