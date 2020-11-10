package cn.ac.iie.spark.rdd

import scala.collection.JavaConverters.asScalaIteratorConverter
import cn.ac.iie.config.ApplicationConfig
import cn.ac.iie.service.update.UpdateDocument
import cn.ac.iie.spark
import cn.ac.iie.spark.partition.QueryArangoPartition
import com.arangodb.ArangoCursor
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class ArangoRdd[T: ClassTag](@transient override val sparkContext: SparkContext,
                             val options: ReadOptions
                            ) extends RDD[T](sparkContext, Nil) {

  private val LOG = LoggerFactory.getLogger(UpdateDocument.getClass)

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {

    createCursor(split.asInstanceOf[QueryArangoPartition]).asScala
  }

  override protected def getPartitions: Array[Partition] = {
    val partitions = ArrayBuffer[Partition]()
    val total = getCountTotal
    for (i <- 0 until ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS) {
      val partition = getPartition(i, total)
      partitions += partition
    }
    partitions.toArray
  }

  private def createCursor(split: QueryArangoPartition)(implicit clazz: ClassTag[T]): ArangoCursor[T] = {

    var arangoCursor:ArangoCursor[T] = null
    val arangoDB = spark.createArangoBuilder(options).build()
    try {
      val offset = split.offset
      val separate = split.separate
      val collection = options.collection
      val sql = s"FOR doc IN $collection limit $offset,$separate RETURN doc"
      LOG.info(sql)
      arangoCursor = arangoDB.db(options.database).query(sql,clazz.runtimeClass.asInstanceOf[Class[T]])
    }catch {
      case e: Exception => LOG.error(s"创建Cursor异常:${e.getMessage}")
    }finally {
      arangoDB.shutdown()
    }
    arangoCursor
  }

  override def repartition(numPartitions: Int)(implicit ord: Ordering[T]): RDD[T] = super.repartition(numPartitions)

  private def getPartition(idx: Int, countTotal: Long): QueryArangoPartition = {
    val sepNum = countTotal / ApplicationConfig.SPARK_SQL_SHUFFLE_PARTITIONS + 1
    val offsetNum = idx * sepNum
    new QueryArangoPartition(idx, offsetNum, sepNum)
  }

  override def count(): Long = getCountTotal

  private def getCountTotal: Long = {
    val arangoDB = spark.createArangoBuilder(options).build()
    var cnt = 0L
    val sql = s"RETURN LENGTH(${options.collection})"
    LOG.info(sql)
    try {
      val longs = arangoDB.db(options.database).query(sql, classOf[Long])
      while (longs.hasNext) cnt = longs.next
    } catch {
      case e: Exception => LOG.error(sql + s"执行异常:${e.getMessage}")
    }finally {
      arangoDB.shutdown()
    }
    cnt
  }
}
