package cn.ac.iie.etl

import cn.ac.iie.utils.{ConfigUtils, InitArangoDBPool}
import com.arangodb.ArangoCursor
import com.arangodb.entity.BaseDocument
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.ClassTag

object CursorTransform {

  /**
    * 将查询ArangoDB的结果集转换为DataFrame
    * @param collection_Name //查询的collection
    * @param class_Type     //转换的pojo类对象
    * @param spark /        /sparkSession引擎
    * @tparam T
    * @return
    */
  def cursorToDataFrame[T:ClassTag](collection_Name:String,class_Type: Class[T],spark:SparkSession): DataFrame ={
    val query = s"FOR doc IN $collection_Name RETURN doc"
    println(query)
    val cursor: ArangoCursor[T] = InitArangoDBPool.arangoDB.db(ConfigUtils.ARANGODB_DB_NAME)
      .query(query, InitArangoDBPool.bindVars, InitArangoDBPool.options, class_Type)

    val cursor_DF = spark.createDataFrame(cursor.asListRemaining(),class_Type)
    cursor_DF.printSchema()

    cursor_DF

  }

}
