package cn.ac.iie.main

import cn.ac.iie.test.Config
import cn.ac.iie.dao.BaseMediaDataLoad
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object IPLearningApplication {
  private val logger: Logger = LoggerFactory.getLogger(IPLearningApplication.getClass)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .config("spark.kryo.classesToRegister","com.tinkerpop.blueprints.impls.orient.OrientGraphFactory")
      .config("spark.network.timeout", "300s")
      .config("spark.sql.shuffle.partitions", Config.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY)
      .master(Config.MASTER)
      .getOrCreate()

    logger.warn("sparkession获取成功！！！")
    BaseMediaDataLoad.loadMediaDate(spark)
//    BaseMediaDataLoad.

  }

}
