package cn.ac.iie.config

import com.typesafe.config.{Config, ConfigFactory}

object ApplicationConfig {
  private lazy val config: Config = ConfigFactory.load()

  val SPARK_SQL_SHUFFLE_PARTITIONS: Int = config.getInt("spark.sql.shuffle.partitions")
  val SPARK_EXECUTOR_MEMORY: String = config.getString("spark.executor.memory")
  val SPARK_APP_NAME: String = config.getString("spark.app.name")
  val SPARK_NETWORK_TIMEOUT: String = config.getString("spark.network.timeout")
//  val REPARTITION_NUMBER: Int = config.getInt("repartitionNumber")
  val MASTER: String = config.getString("master")
  val SPARK_SERIALIZER: String = config.getString("spark.serializer")

  val NUMPARTITIONS: String = config.getString("spark.read.clickhouse.numPartitions")
  val SPARK_READ_CLICKHOUSE_URL: String = config.getString("spark.read.clickhouse.url")
  val SPARK_READ_CLICKHOUSE_DRIVER: String = config.getString("spark.read.clickhouse.driver")
  val SPARK_READ_CLICKHOUSE_USER: String = config.getString("spark.read.clickhouse.user")
  val SPARK_READ_CLICKHOUSE_PASSWORD: String = config.getString("spark.read.clickhouse.password")
  val SPARK_READ_CLICKHOUSE_FETCHSIZE: String = config.getString("spark.read.clickhouse.fetchsize")
  val SPARK_READ_CLICKHOUSE_PARTITIONCOLUMN: String = config.getString("spark.read.clickhouse.partitionColumn")

  val ARANGODB_HOST: String= config.getString("arangoDB.host")
  val ARANGODB_PORT: Int = config.getInt("arangoDB.port")
  val ARANGODB_USER: String= config.getString("arangoDB.user")
  val ARANGODB_PASSWORD:String= config.getString("arangoDB.password")
  val ARANGODB_DB_NAME:String= config.getString("arangoDB.DB.name")
  val ARANGODB_TTL: Int = config.getInt("arangoDB.ttl")
  val CLICKHOUSE_SOCKET_TIMEOUT: Int = config.getInt("clickhouse.socket.timeout")

  val THREAD_POOL_NUMBER: Int = config.getInt("thread.pool.number")

  val CLICKHOUSE_TIME_LIMIT_TYPE: Int = config.getInt("clickhouse.time.limit.type")

  val READ_CLICKHOUSE_MAX_TIME: Long = config.getLong("read.clickhouse.max.time")
  val READ_CLICKHOUSE_MIN_TIME: Long = config.getLong("read.clickhouse.min.time")

  val ARANGODB_READ_LIMIT: Int = config.getInt("arangoDB.read.limit")
  val UPDATE_ARANGO_BATCH: Int = config.getInt("update.arango.batch")
  val RECENT_COUNT_HOUR: Int = config.getInt("recent.count.hour")
  val DISTINCT_CLIENT_IP_NUM: Int = config.getInt("distinct.client.ip.num")

  val UPDATE_INTERVAL: Int = config.getInt("update.interval")

}
