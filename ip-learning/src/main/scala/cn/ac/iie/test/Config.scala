package cn.ac.iie.test

import com.typesafe.config.{Config, ConfigFactory}

object Config {
  private lazy val config: Config = ConfigFactory.load()
  val SPARK_SQL_SHUFFLE_PARTITIONS: String = config.getString("spark.sql.shuffle.partitions")
  val SPARK_SQL_READ_FETCHSIZE: String = config.getString("spark.sql.read.fetchsize")
  val SPARK_EXECUTOR_MEMORY: String = config.getString("spark.executor.memory")
  val NUMPARTITIONS: String = config.getString("numPartitions")
  val MASTER: String = config.getString("master")
  val MAXPOOLSIZE: Int = config.getInt("maxPoolSize")
  val MINTIME: String = config.getString("minTime")
  val MAXTIME: String = config.getString("maxTime")

  val REPARTITION_NUMBER: Int = config.getInt("repartitionNumber")
  val ARANGODB_BATCH: Int = config.getInt("arangoDB.batch")
  val ARANGODB_TTL: Int = config.getInt("arangoDB.ttl")
  val CLICKHOUSE_SOCKET_TIMEOUT: Int = config.getInt("clickhouse.socket.timeout")
  val SPARK_SERIALIZER: String = config.getString("spark.serializer")

}
