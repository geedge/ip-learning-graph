package cn.ac.iie.utils

import cn.ac.iie.config.ApplicationConfig
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkSessionUtil {
  private val LOG = LoggerFactory.getLogger(SparkSessionUtil.getClass)

  val spark: SparkSession = getSparkSession

  private def getSparkSession: SparkSession ={
    val spark: SparkSession = SparkSession
      .builder()
      .appName(ApplicationConfig.SPARK_APP_NAME)
      .config("spark.serializer", ApplicationConfig.SPARK_SERIALIZER)
      .config("spark.network.timeout", ApplicationConfig.SPARK_NETWORK_TIMEOUT)
      .config("spark.sql.shuffle.partitions", ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.executor.memory", ApplicationConfig.SPARK_EXECUTOR_MEMORY)
      .master(ApplicationConfig.MASTER)
      .getOrCreate()
    LOG.warn("sparkession获取成功！！！")
    spark
  }

  def closeSpark(): Unit ={
    if (spark != null){
      spark.stop()
    }
  }

}
