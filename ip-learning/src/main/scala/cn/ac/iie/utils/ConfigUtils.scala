package cn.ac.iie.utils

import com.typesafe.config.{Config, ConfigFactory}

object ConfigUtils {
  private lazy val config: Config = ConfigFactory.load()

  val SPARK_SQL_SHUFFLE_PARTITIONS: String = config.getString("spark.sql.shuffle.partitions")
  val SPARK_SQL_READ_FETCHSIZE: String = config.getString("spark.sql.read.fetchsize")
  val SPARK_EXECUTOR_MEMORY: String = config.getString("spark.executor.memory")
  val SPARK_APP_NAME: String = config.getString("spark.app.name")
  val SPARK_NETWORK_TIMEOUT: String = config.getString("spark.network.timeout")
  val REPARTITION_NUMBER: Int = config.getInt("repartitionNumber")
  val SPARK_SERIALIZER: String = config.getString("spark.serializer")

  val NUMPARTITIONS: String = config.getString("numPartitions")
  val MASTER: String = config.getString("master")
  val MAXPOOLSIZE: Int = config.getInt("maxPoolSize")
  val MINTIME: String = config.getString("minTime")
  val MAXTIME: String = config.getString("maxTime")

  val ARANGODB_HOST: String= config.getString("arangoDB.host")
  val ARANGODB_PORT: Int = config.getInt("arangoDB.port")
  val ARANGODB_USER: String= config.getString("arangoDB.user")
  val ARANGODB_PASSWORD:String= config.getString("arangoDB.password")
  val ARANGODB_BATCH: Int = config.getInt("arangoDB.batch")
  val ARANGODB_DB_NAME:String= config.getString("arangoDB.DB.name")
  val ARANGODB_TTL: Int = config.getInt("arangoDB.ttl")
  val CLICKHOUSE_SOCKET_TIMEOUT: Int = config.getInt("clickhouse.socket.timeout")

  val THREAD_POOL_NUMBER: Int = config.getInt("thread.pool.number")


}
