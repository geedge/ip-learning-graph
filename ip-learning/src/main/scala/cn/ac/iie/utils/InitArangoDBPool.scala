package cn.ac.iie.utils

import java.util

import com.arangodb.ArangoDB
import com.arangodb.model.AqlQueryOptions
import com.arangodb.util.MapBuilder

object InitArangoDBPool {

  @transient
  lazy val arangoDB: ArangoDB = new ArangoDB.Builder()
    .maxConnections(ConfigUtils.MAXPOOLSIZE)
    .host(ConfigUtils.ARANGODB_HOST, ConfigUtils.ARANGODB_PORT)
    .user(ConfigUtils.ARANGODB_USER)
    .password(ConfigUtils.ARANGODB_PASSWORD)
    .build

  val bindVars: util.Map[String, AnyRef] = new MapBuilder().get

  val options: AqlQueryOptions = new AqlQueryOptions()
    .ttl(ConfigUtils.ARANGODB_TTL)

}
