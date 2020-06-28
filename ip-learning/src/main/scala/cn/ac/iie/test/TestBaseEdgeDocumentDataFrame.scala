package cn.ac.iie.test

import cn.ac.iie.dao.{BaseMediaDataLoad, UpdateArangoGraphByDF}
import cn.ac.iie.utils.InitArangoDBPool
import com.arangodb.entity.BaseEdgeDocument
import com.arangodb.util.MapBuilder
import com.arangodb.{ArangoCursor, ArangoDB}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object TestBaseEdgeDocumentDataFrame {
  @transient
  var arangoDB: ArangoDB = _
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.network.timeout", "300s")
      .config("spark.sql.shuffle.partitions", Config.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY)
      .master(Config.MASTER)
      .getOrCreate()

    BaseMediaDataLoad.loadMediaDate(spark)
    val e_Address_v_FQDN_to_v_IP_DF = BaseMediaDataLoad.getFQDNAddressIPEdgeFromMedia(spark)
    UpdateArangoGraphByDF.updateFQDNAddressIPEdge(e_Address_v_FQDN_to_v_IP_DF,spark)

    InitArangoDBPool.arangoDB.shutdown()
    spark.close()


  }

}
