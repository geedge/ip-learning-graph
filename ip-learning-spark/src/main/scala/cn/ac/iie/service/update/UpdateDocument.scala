package cn.ac.iie.service.update

import java.util
import java.util.concurrent.ConcurrentHashMap

import cn.ac.iie.config.ApplicationConfig
import cn.ac.iie.dao.BaseArangoData
import cn.ac.iie.dao.BaseArangoData._
import cn.ac.iie.service.transform.MergeDataFrame._
import cn.ac.iie.service.update.UpdateDocHandler._
import cn.ac.iie.utils.{ArangoDBConnect, ExecutorThreadPool, SparkSessionUtil}
import cn.ac.iie.utils.SparkSessionUtil.spark
import com.arangodb.entity.{BaseDocument, BaseEdgeDocument}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import scala.collection.mutable.WrappedArray.ofRef

object UpdateDocument {
  private val pool = ExecutorThreadPool.getInstance
  private val arangoManger: ArangoDBConnect = ArangoDBConnect.getInstance()
  private val LOG = LoggerFactory.getLogger(UpdateDocument.getClass)
  private val baseArangoData = new BaseArangoData()

  def update(): Unit = {
    try {
      updateDocument("FQDN", historyVertexFqdnMap, getVertexFqdnRow, classOf[BaseDocument], mergeVertexFqdn)
      updateDocument("IP", historyVertexIpMap, getVertexIpRow, classOf[BaseDocument], mergeVertexIp)
      updateDocument("R_LOCATE_FQDN2IP", historyRelationFqdnAddressIpMap, getRelationFqdnLocateIpRow, classOf[BaseEdgeDocument], mergeRelationFqdnLocateIp)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      pool.shutdown()
      arangoManger.clean()
      SparkSessionUtil.closeSpark()
    }
  }

  private def updateDocument[T <: BaseDocument](collName: String,
                                                historyMap: ConcurrentHashMap[Integer, ConcurrentHashMap[String, T]],
                                                getDocumentRow: (Row, ConcurrentHashMap[String, T]) => T,
                                                clazz: Class[T],
                                                getNewDataRdd: () => RDD[Row]
                                               ): Unit = {
    baseArangoData.readHistoryData(collName, historyMap, clazz)
    val hisBc = spark.sparkContext.broadcast(historyMap)
    try {
      val start = System.currentTimeMillis()
      val newDataRdd = getNewDataRdd()
      newDataRdd.foreachPartition(iter => {
        val partitionId: Int = TaskContext.get.partitionId
        val dictionaryMap: ConcurrentHashMap[String, T] = hisBc.value.get(partitionId)
        val resultDocumentList = new util.ArrayList[T]
        var i = 0
        iter.foreach(row => {
          val document = getDocumentRow(row, dictionaryMap)
          resultDocumentList.add(document)
          i += 1
          if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH) {
            arangoManger.overwrite(resultDocumentList, collName)
            LOG.warn(s"更新:$collName" + i)
            i = 0
          }
        })
        if (i != 0) {
          arangoManger.overwrite(resultDocumentList, collName)
          LOG.warn(s"更新$collName:" + i)
        }
      })
      val last = System.currentTimeMillis()
      LOG.warn(s"更新$collName 时间：${last - start}")
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      hisBc.destroy()
    }
  }

  private def getVertexFqdnRow(row: Row, dictionaryMap: ConcurrentHashMap[String, BaseDocument]): BaseDocument = {
    val fqdn = row.getAs[String]("FQDN")
    val lastFoundTime = row.getAs[Long]("LAST_FOUND_TIME")
    val firstFoundTime = row.getAs[Long]("FIRST_FOUND_TIME")
    var document: BaseDocument = dictionaryMap.getOrDefault(fqdn, null)
    if (document != null) {
      updateMaxAttribute(document, lastFoundTime, "LAST_FOUND_TIME")
    } else {
      document = new BaseDocument
      document.setKey(fqdn)
      document.addAttribute("FQDN_NAME", fqdn)
      document.addAttribute("FIRST_FOUND_TIME", firstFoundTime)
      document.addAttribute("LAST_FOUND_TIME", lastFoundTime)
    }
    document
  }

  private def getVertexIpRow(row: Row, dictionaryMap: ConcurrentHashMap[String, BaseDocument]): BaseDocument = {
    val ip = row.getAs[String]("IP")
    val firstFoundTime = row.getAs[Long]("FIRST_FOUND_TIME")
    val lastFoundTime = row.getAs[Long]("LAST_FOUND_TIME")
    val sessionCountList = row.getAs[ofRef[AnyRef]]("SESSION_COUNT_LIST")
    val bytesSumList = row.getAs[ofRef[AnyRef]]("BYTES_SUM_LIST")
    val ipTypeList = row.getAs[ofRef[String]]("ip_type_list")
    val sepAttributeTuple = separateAttributeByIpType(ipTypeList, sessionCountList, bytesSumList)

    var document = dictionaryMap.getOrDefault(ip, null)
    if (document != null) {
      updateMaxAttribute(document, lastFoundTime, "LAST_FOUND_TIME")
      updateSumAttribute(document, sepAttributeTuple._1, "SERVER_SESSION_COUNT")
      updateSumAttribute(document, sepAttributeTuple._2, "SERVER_BYTES_SUM")
      updateSumAttribute(document, sepAttributeTuple._3, "CLIENT_SESSION_COUNT")
      updateSumAttribute(document, sepAttributeTuple._4, "CLIENT_BYTES_SUM")
    } else {
      document = new BaseDocument
      document.setKey(ip)
      document.addAttribute("IP", ip)
      document.addAttribute("FIRST_FOUND_TIME", firstFoundTime)
      document.addAttribute("LAST_FOUND_TIME", lastFoundTime)
      document.addAttribute("SERVER_SESSION_COUNT", sepAttributeTuple._1)
      document.addAttribute("SERVER_BYTES_SUM", sepAttributeTuple._2)
      document.addAttribute("CLIENT_SESSION_COUNT", sepAttributeTuple._3)
      document.addAttribute("CLIENT_BYTES_SUM", sepAttributeTuple._4)
      document.addAttribute("COMMON_LINK_INFO", "")
    }
    document
  }

  private def getRelationFqdnLocateIpRow(row: Row, dictionaryMap: ConcurrentHashMap[String, BaseEdgeDocument]): BaseEdgeDocument = {
    val fqdn = row.getAs[String]("FQDN")
    val serverIp = row.getAs[String]("common_server_ip")
    val firstFoundTime = row.getAs[Long]("FIRST_FOUND_TIME")
    val lastFoundTime = row.getAs[Long]("LAST_FOUND_TIME")
    val countTotalList = row.getAs[ofRef[AnyRef]]("COUNT_TOTAL_LIST")
    val schemaTypeList = row.getAs[ofRef[AnyRef]]("schema_type_list")
    val distCipRecent = row.getAs[ofRef[ofRef[String]]]("DIST_CIP_RECENT")

    val sepAttritubeMap: Map[String, Long] = separateAttributeByProtocol(schemaTypeList, countTotalList)
    val distinctIp: Array[String] = mergeDistinctIp(distCipRecent)

    val key = fqdn.concat("-" + serverIp)
    var document = dictionaryMap.getOrDefault(key, null)
    if (document != null) {
      updateMaxAttribute(document, lastFoundTime, "LAST_FOUND_TIME")
      updateProtocolAttritube(document, sepAttritubeMap)
      updateDistinctIp(document, distinctIp)
    } else {
      document = new BaseEdgeDocument()
      document.setKey(key)
      document.setFrom("FQDN/" + fqdn)
      document.setTo("IP/" + serverIp)
      document.addAttribute("FIRST_FOUND_TIME", firstFoundTime)
      document.addAttribute("LAST_FOUND_TIME", lastFoundTime)
      putProtocolAttritube(document, sepAttritubeMap)
      putDistinctIp(document, distinctIp)
    }
    document
  }

}
