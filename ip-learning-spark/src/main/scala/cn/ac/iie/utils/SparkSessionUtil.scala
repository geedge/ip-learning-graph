package cn.ac.iie.utils

import cn.ac.iie.config.ApplicationConfig
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.slf4j.LoggerFactory

object SparkSessionUtil {
  private val LOG = LoggerFactory.getLogger(SparkSessionUtil.getClass)

  val spark: SparkSession = getSparkSession

  var sparkContext: SparkContext = getContext

  private def getSparkSession: SparkSession ={
    val spark: SparkSession = SparkSession
      .builder()
      .appName(ApplicationConfig.SPARK_APP_NAME)
      .config("spark.serializer", ApplicationConfig.SPARK_SERIALIZER)
      .config("spark.network.timeout", ApplicationConfig.SPARK_NETWORK_TIMEOUT)
      .config("spark.sql.shuffle.partitions", ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.executor.memory", ApplicationConfig.SPARK_EXECUTOR_MEMORY)
      .config("arangodb.hosts", s"${ApplicationConfig.ARANGODB_HOST}:${ApplicationConfig.ARANGODB_PORT}")
      .config("arangodb.user", ApplicationConfig.ARANGODB_USER)
      .config("arangodb.password", ApplicationConfig.ARANGODB_PASSWORD)
      .master(ApplicationConfig.MASTER)
      .getOrCreate()
    LOG.warn("sparkession获取成功！！！")
    spark
  }

  def getContext: SparkContext = {
    @transient var sc: SparkContext = null
      if (sparkContext == null) sc = spark.sparkContext
    sc
  }

  def getLongAccumulator(name: String): LongAccumulator ={
    if (sparkContext == null){
      sparkContext = getContext
    }
    sparkContext.longAccumulator(name)

  }

  def closeSpark(): Unit ={
    if (spark != null){
      spark.stop()
    }
    if (sparkContext != null){
      sparkContext.stop()
    }
  }

}
