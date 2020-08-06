package cn.ac.iie.service.update
import java.util

import cn.ac.iie.config.ApplicationConfig
import cn.ac.iie.dao.BaseArangoData
import cn.ac.iie.dao.BaseArangoData._
import cn.ac.iie.service.transform.MergeDataFrame._
import cn.ac.iie.service.update.UpdateDocHandler._
import cn.ac.iie.utils.ArangoDBConnect
import cn.ac.iie.utils.SparkSessionUtil.spark
import com.arangodb.entity.{BaseDocument, BaseEdgeDocument}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import scala.collection.mutable.WrappedArray.ofRef

object UpdateDocument {

  val arangoManger: ArangoDBConnect = ArangoDBConnect.getInstance()
  private val LOG = LoggerFactory.getLogger(UpdateDocument.getClass)
  private val baseArangoData = new BaseArangoData()

  def updateVertexFqdn(): Unit ={
    baseArangoData.readHistoryData("FQDN",historyVertexFqdnMap,classOf[BaseDocument])
    val hisVerFqdnBc = spark.sparkContext.broadcast(historyVertexFqdnMap)
    try {
      val start = System.currentTimeMillis()
      val mergeVertexFqdnDf: RDD[Row] = mergeVertexFqdn()
      mergeVertexFqdnDf.foreachPartition(iter => {
        val partitionId: Int = TaskContext.get.partitionId
        val hisVerFqdnMapTmp = hisVerFqdnBc.value.get(partitionId)
        val resultDocumentList: util.ArrayList[BaseDocument] = new util.ArrayList[BaseDocument]
        var i = 0
        iter.foreach(row => {
          val fqdn = row.getAs[String]("FQDN")
          val lastFoundTime = row.getAs[Long]("LAST_FOUND_TIME")
          val firstFoundTime = row.getAs[Long]("FIRST_FOUND_TIME")
          var document: BaseDocument = hisVerFqdnMapTmp.getOrDefault(fqdn,null)
          if (document != null){
            updateMaxAttribute(document,lastFoundTime,"LAST_FOUND_TIME")
          } else{
            document = new BaseDocument
            document.setKey(fqdn)
            document.addAttribute("FQDN_NAME",fqdn)
            document.addAttribute("FIRST_FOUND_TIME",firstFoundTime)
            document.addAttribute("LAST_FOUND_TIME",lastFoundTime)
          }
          resultDocumentList.add(document)
          i+=1
          if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH){
            arangoManger.overwrite(resultDocumentList, "FQDN")
            LOG.warn("更新FQDN:" + i)
            i = 0
          }
        })
        if (i != 0) {
          arangoManger.overwrite(resultDocumentList, "FQDN")
          LOG.warn("更新FQDN:" + i)
        }
      })
      val last = System.currentTimeMillis()
      LOG.warn(s"更新FQDN时间：${last-start}")
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      hisVerFqdnBc.destroy()
    }
  }

  def updateVertexIp(): Unit ={
    baseArangoData.readHistoryData("IP",historyVertexIpMap,classOf[BaseDocument])
    val hisVerIpBc = spark.sparkContext.broadcast(historyVertexIpMap)
    try {
      val start = System.currentTimeMillis()
      val mergeVertexIpDf = mergeVertexIp()
      mergeVertexIpDf.foreachPartition(iter => {
        val partitionId: Int = TaskContext.get.partitionId
        val hisVerIpMapTmp = hisVerIpBc.value.get(partitionId)
        val resultDocumentList: util.ArrayList[BaseDocument] = new util.ArrayList[BaseDocument]
        var i = 0
        iter.foreach(row => {
          val ip = row.getAs[String]("IP")
          val firstFoundTime = row.getAs[Long]("FIRST_FOUND_TIME")
          val lastFoundTime = row.getAs[Long]("LAST_FOUND_TIME")
          val sessionCountList = row.getAs[ofRef[AnyRef]]("SESSION_COUNT_LIST")
          val bytesSumList = row.getAs[ofRef[AnyRef]]("BYTES_SUM_LIST")
          val ipTypeList = row.getAs[ofRef[String]]("ip_type_list")
          val sepAttributeTuple = separateAttributeByIpType(ipTypeList,sessionCountList,bytesSumList)

          var document = hisVerIpMapTmp.getOrDefault(ip,null)
          if (document != null){
            updateMaxAttribute(document,lastFoundTime,"LAST_FOUND_TIME")
            updateSumAttribute(document,sepAttributeTuple._1,"SERVER_SESSION_COUNT")
            updateSumAttribute(document,sepAttributeTuple._2,"SERVER_BYTES_SUM")
            updateSumAttribute(document,sepAttributeTuple._3,"CLIENT_SESSION_COUNT")
            updateSumAttribute(document,sepAttributeTuple._4,"CLIENT_BYTES_SUM")
          } else {
            document = new BaseDocument
            document.setKey(ip)
            document.addAttribute("IP",ip)
            document.addAttribute("FIRST_FOUND_TIME",firstFoundTime)
            document.addAttribute("LAST_FOUND_TIME",lastFoundTime)
            document.addAttribute("SERVER_SESSION_COUNT",sepAttributeTuple._1)
            document.addAttribute("SERVER_BYTES_SUM",sepAttributeTuple._2)
            document.addAttribute("CLIENT_SESSION_COUNT",sepAttributeTuple._3)
            document.addAttribute("CLIENT_BYTES_SUM",sepAttributeTuple._4)
            document.addAttribute("COMMON_LINK_INFO","")
          }
          resultDocumentList.add(document)
          i+=1
          if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH){
            arangoManger.overwrite(resultDocumentList, "IP")
            LOG.warn("更新IP:" + i)
            i = 0
          }
        })
        if (i != 0) {
          arangoManger.overwrite(resultDocumentList, "IP")
          LOG.warn("更新IP:" + i)
        }
      })
      val last = System.currentTimeMillis()
      LOG.warn(s"更新IP时间：${last-start}")
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      hisVerIpBc.destroy()
    }
  }

  def updateRelationFqdnLocateIp(): Unit ={
    baseArangoData.readHistoryData("R_LOCATE_FQDN2IP", historyRelationFqdnAddressIpMap, classOf[BaseEdgeDocument])
    val hisReFqdnLocIpBc = spark.sparkContext.broadcast(historyRelationFqdnAddressIpMap)
    try {
      val start = System.currentTimeMillis()
      val mergeRelationFqdnLocateIpDf = mergeRelationFqdnLocateIp()
      mergeRelationFqdnLocateIpDf.foreachPartition(iter => {
        val partitionId: Int = TaskContext.get.partitionId
        val hisRelaFqdnLocaIpMapTmp = hisReFqdnLocIpBc.value.get(partitionId)
        val resultDocumentList: util.ArrayList[BaseEdgeDocument] = new util.ArrayList[BaseEdgeDocument]
        var i = 0
        iter.foreach(row => {
          val fqdn = row.getAs[String]("FQDN")
          val serverIp = row.getAs[String]("common_server_ip")
          val firstFoundTime = row.getAs[Long]("FIRST_FOUND_TIME")
          val lastFoundTime = row.getAs[Long]("LAST_FOUND_TIME")
          val countTotalList = row.getAs[ofRef[AnyRef]]("COUNT_TOTAL_LIST")
          val schemaTypeList = row.getAs[ofRef[AnyRef]]("schema_type_list")
          val distCipRecent = row.getAs[ofRef[ofRef[String]]]("DIST_CIP_RECENT")

          val sepAttritubeMap: Map[String, Long] = separateAttributeByProtocol(schemaTypeList,countTotalList)
          val distinctIp: Array[String] = mergeDistinctIp(distCipRecent)

          val key = fqdn.concat("-"+serverIp)
          var document: BaseEdgeDocument = hisRelaFqdnLocaIpMapTmp.getOrDefault(key,null)
          if (document != null){
            updateMaxAttribute(document,lastFoundTime,"LAST_FOUND_TIME")
            updateProtocolAttritube(document,sepAttritubeMap)
            updateDistinctIp(document,distinctIp)
          }else {
            document = new BaseEdgeDocument()
            document.setKey(key)
            document.setFrom("FQDN/" + fqdn)
            document.setTo("IP/" + serverIp)
            document.addAttribute("FIRST_FOUND_TIME", firstFoundTime)
            document.addAttribute("LAST_FOUND_TIME", lastFoundTime)
            putProtocolAttritube(document,sepAttritubeMap)
            putDistinctIp(document,distinctIp)
          }
          resultDocumentList.add(document)
          i+=1
          if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH){
            arangoManger.overwrite(resultDocumentList, "R_LOCATE_FQDN2IP")
            LOG.warn("更新R_LOCATE_FQDN2IP:" + i)
            i = 0
          }
        })
        if (i != 0) {
          arangoManger.overwrite(resultDocumentList, "R_LOCATE_FQDN2IP")
          LOG.warn("更新R_LOCATE_FQDN2IP:" + i)
        }
      })
      val last = System.currentTimeMillis()
      LOG.warn(s"更新R_LOCATE_FQDN2IP时间：${last-start}")
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      hisReFqdnLocIpBc.destroy()
    }
  }

}
