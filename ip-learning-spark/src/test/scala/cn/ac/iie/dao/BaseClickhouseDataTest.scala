package cn.ac.iie.dao

import cn.ac.iie.utils.SparkSessionUtil
import org.apache.spark.sql.SparkSession


object BaseClickhouseDataTest {
  private val spark: SparkSession = SparkSessionUtil.spark
  def main(args: Array[String]): Unit = {
//    BaseClickhouseData loadConnectionDataFromCk()
    val sql =
      """
        |SELECT
        | FQDN,MAX( LAST_FOUND_TIME ) AS LAST_FOUND_TIME,MIN( FIRST_FOUND_TIME ) AS FIRST_FOUND_TIME
        |FROM
        | (
        |  (SELECT
        |    ssl_sni AS FQDN,MAX( common_recv_time ) AS LAST_FOUND_TIME,MIN( common_recv_time ) AS FIRST_FOUND_TIME
        |   FROM
        |    global_temp.dbtable
        |   WHERE
        |    common_schema_type = 'SSL' GROUP BY ssl_sni
        |  )
        |  UNION ALL
        |  (SELECT
        |    http_host AS FQDN,MAX( common_recv_time ) AS LAST_FOUND_TIME,MIN( common_recv_time ) AS FIRST_FOUND_TIME
        |   FROM
        |    global_temp.dbtable
        |   WHERE
        |    common_schema_type = 'HTTP' GROUP BY http_host
        |  )
        | )
        |GROUP BY
        | FQDN
        |HAVING
        | FQDN != ''
      """.stripMargin
    println(sql)
    val vertexFqdnDf = spark.sql(sql)
    vertexFqdnDf.show(10)
  }

}
