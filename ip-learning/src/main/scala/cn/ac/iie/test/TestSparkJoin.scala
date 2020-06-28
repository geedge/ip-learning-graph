package cn.ac.iie.test

import cn.ac.iie.dao.BaseMediaDataLoad
import cn.ac.iie.etl.CursorTransform
import cn.ac.iie.pojo.BaseVertexFqdn
import cn.ac.iie.utils.InitArangoDBPool
import com.arangodb.ArangoCursor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object TestSparkJoin {
  private val logger: Logger = LoggerFactory.getLogger(TestSparkJoin.getClass)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .config("spark.serializer", Config.SPARK_SERIALIZER)
      .config("spark.network.timeout", "300s")
      .config("spark.sql.shuffle.partitions", Config.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY)
      .master(Config.MASTER)
      .getOrCreate()
    logger.warn("sparkession获取成功！！！")
    BaseMediaDataLoad.loadMediaDate(spark)
//    val v_FQDN_DF = BaseMediaDataLoad.getFQDNVertexFromMedia(spark)
    BaseMediaDataLoad.getFQDNAddressIPEdgeFromMedia(spark).show(10)
//    v_FQDN_DF.printSchema()


/*
    val arangoDB = InitArangoDBPool.arangoDB
    val options = InitArangoDBPool.options
    val bindVars = InitArangoDBPool.bindVars

//    val v_FQDN_Query = "FOR doc IN V_FQDN limit 100 RETURN doc"
//    val v_FQDN_Query = "FOR doc IN V_FQDN RETURN doc"
//    val v_FQDN_Cursor: ArangoCursor[BaseVertexFqdn] = arangoDB.db("insert_iplearn_noindex")
//      .query(v_FQDN_Query, bindVars, options, classOf[BaseVertexFqdn])
//    val v_FQDN_Curson_DF: DataFrame = spark.createDataFrame(v_FQDN_Cursor.asListRemaining(),classOf[BaseVertexFqdn])
//    v_FQDN_Curson_DF.printSchema()
//
    val v_FQDN_Curson_DF = CursorTransform.cursorToDataFrame("V_FQDN",classOf[BaseVertexFqdn],spark)
    val v_Fqdn_Join_Df = v_FQDN_DF.join(v_FQDN_Curson_DF,v_FQDN_DF("new_fqdn_name")===v_FQDN_Curson_DF("key"),"fullouter")
    v_Fqdn_Join_Df.printSchema()
    v_Fqdn_Join_Df
//      .filter(row => row.getAs[String]("new_fqdn_name")!=null)
//      .filter(row => row.getAs[String]("new_fqdn_name")!=null)
      .show(300)

    arangoDB.shutdown()
    spark.close()
*/
  }

}
