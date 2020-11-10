package cn.ac.iie.service.update

import java.util

import cn.ac.iie.config.ApplicationConfig
import cn.ac.iie.service.transform.MergeDataFrame._
import cn.ac.iie.service.update.UpdateDocHandler._
import cn.ac.iie.utils.{ArangoDBConnect, SparkSessionUtil}
import com.arangodb.entity.{BaseDocument, BaseEdgeDocument}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import scala.collection.mutable.WrappedArray.ofRef

object UpdateDocument {
  private val arangoManger: ArangoDBConnect = ArangoDBConnect.getInstance()
  private val LOG = LoggerFactory.getLogger(UpdateDocument.getClass)

  def update(): Unit = {
    try {
      updateDocument("FQDN", getVertexFqdnRow, mergeVertexFqdn)

      updateDocument("SUBSCRIBER",getVertexSubidRow,mergeVertexSubid)

      insertFrameIp()

      updateDocument("R_LOCATE_SUBSCRIBER2IP",getRelationSubidLocateIpRow,mergeRelationSubidLocateIp)

      updateDocument("R_LOCATE_FQDN2IP", getRelationFqdnLocateIpRow, mergeRelationFqdnLocateIp)

      updateDocument("IP", getVertexIpRow, mergeVertexIp)

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      arangoManger.clean()
      SparkSessionUtil.closeSpark()
      System.exit(0)
    }
  }

  private def updateDocument[T <: BaseDocument](collName: String,
                                                getDocumentRow: ((String, (Option[T], Option[Row]))) => T,
                                                getJoinRdd: () => RDD[(String, (Option[T], Option[Row]))]
                                               ): Unit = {
    try {
      val start = System.currentTimeMillis()
      val joinRdd = getJoinRdd()
      joinRdd.foreachPartition(iter => {
        val resultDocumentList = new util.ArrayList[T]
        var i = 0
        iter.foreach(row => {
          val document = getDocumentRow(row)
          if (document != null){
            resultDocumentList.add(document)
          }
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
    }
  }

  private def insertFrameIp(): Unit ={
    mergeVertexFrameIp.foreachPartition(iter => {
      val resultDocumentList = new util.ArrayList[BaseDocument]
      var i = 0
      iter.foreach(row => {
        val document = getVertexFrameipRow(row)
        resultDocumentList.add(document)
        i += 1
        if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH) {
          arangoManger.overwrite(resultDocumentList, "IP")
          LOG.warn(s"更新:IP" + i)
          i = 0
        }
      })
      if (i != 0) {
        arangoManger.overwrite(resultDocumentList, "IP")
        LOG.warn(s"更新IP:" + i)
      }
    })
  }

  private def getVertexFrameipRow(row: Row): BaseDocument ={
    val ip = row.getAs[String]("radius_framed_ip")
    val document = new BaseDocument()
    document.setKey(ip)
    document.addAttribute("IP",ip)
    document
  }

  private def getRelationSubidLocateIpRow(joinRow: (String, (Option[BaseEdgeDocument], Option[Row]))): BaseEdgeDocument ={

    val subidLocIpDocOpt = joinRow._2._1
    var subidLocIpDoc = subidLocIpDocOpt match {
      case Some(doc) => doc
      case None => null
    }

    val subidLocIpRowOpt = joinRow._2._2

    val subidLocIpRow = subidLocIpRowOpt match {
      case Some(r) => r
      case None => null
    }

    if (subidLocIpRow != null){
      val subId = subidLocIpRow.getAs[String]("common_subscriber_id")
      val ip = subidLocIpRow.getAs[String]("radius_framed_ip")
      val lastFoundTime = subidLocIpRow.getAs[Long]("LAST_FOUND_TIME")
      val firstFoundTime = subidLocIpRow.getAs[Long]("FIRST_FOUND_TIME")

      val key = subId.concat("-"+ip)
      if (subidLocIpDoc != null){
        updateMaxAttribute(subidLocIpDoc,lastFoundTime,"LAST_FOUND_TIME")
      } else {
        subidLocIpDoc = new BaseEdgeDocument()
        subidLocIpDoc.setKey(key)
        subidLocIpDoc.setFrom("SUBSCRIBER/" + subId)
        subidLocIpDoc.setTo("IP/" + ip)
        subidLocIpDoc.addAttribute("SUBSCRIBER",subId)
        subidLocIpDoc.addAttribute("IP",ip)
        subidLocIpDoc.addAttribute("FIRST_FOUND_TIME",firstFoundTime)
        subidLocIpDoc.addAttribute("LAST_FOUND_TIME",lastFoundTime)
      }
    }
    subidLocIpDoc
  }

  private def getVertexSubidRow(joinRow: (String, (Option[BaseDocument], Option[Row]))): BaseDocument ={
    val subidDocOpt = joinRow._2._1
    var subidDoc = subidDocOpt match {
      case Some(doc) => doc
      case None => null
    }

    val subidRowOpt = joinRow._2._2

    val subidRow = subidRowOpt match {
      case Some(r) => r
      case None => null
    }

    if (subidRow != null){
      val subId = subidRow.getAs[String]("common_subscriber_id")
      val subLastFoundTime = subidRow.getAs[Long]("LAST_FOUND_TIME")
      val subFirstFoundTime = subidRow.getAs[Long]("FIRST_FOUND_TIME")
      if (subidDoc != null){
        updateMaxAttribute(subidDoc,subLastFoundTime,"LAST_FOUND_TIME")
      } else {
        subidDoc = new BaseDocument()
        subidDoc.setKey(subId)
        subidDoc.addAttribute("SUBSCRIBER",subId)
        subidDoc.addAttribute("FIRST_FOUND_TIME",subFirstFoundTime)
        subidDoc.addAttribute("LAST_FOUND_TIME",subLastFoundTime)
      }
    }

    subidDoc
  }

  private def getVertexFqdnRow(joinRow: (String, (Option[BaseDocument], Option[Row]))): BaseDocument = {
    val fqdnDocOpt = joinRow._2._1
    var fqdnDoc = fqdnDocOpt match {
      case Some(doc) => doc
      case None => null
    }

    val fqdnRowOpt = joinRow._2._2

    val fqdnRow = fqdnRowOpt match {
      case Some(r) => r
      case None => null
    }

    if (fqdnRow != null){
      val fqdn = fqdnRow.getAs[String]("FQDN")
      val lastFoundTime = fqdnRow.getAs[Long]("LAST_FOUND_TIME")
      val firstFoundTime = fqdnRow.getAs[Long]("FIRST_FOUND_TIME")
      if (fqdnDoc != null) {
        updateMaxAttribute(fqdnDoc, lastFoundTime, "LAST_FOUND_TIME")
      } else {
        fqdnDoc = new BaseDocument
        fqdnDoc.setKey(fqdn)
        fqdnDoc.addAttribute("FQDN_NAME", fqdn)
        fqdnDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime)
        fqdnDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime)
      }
    }

    fqdnDoc
  }

  private def getVertexIpRow(joinRow: (String, (Option[BaseDocument], Option[Row]))): BaseDocument = {
    val ipDocOpt = joinRow._2._1
    var ipDoc = ipDocOpt match {
      case Some(doc) => doc
      case None => null
    }

    val ipRowOpt = joinRow._2._2

    val ipRow = ipRowOpt match {
      case Some(r) => r
      case None => null
    }

    if (ipRow != null){
      val ip = ipRow.getAs[String]("IP")
      val firstFoundTime = ipRow.getAs[Long]("FIRST_FOUND_TIME")
      val lastFoundTime = ipRow.getAs[Long]("LAST_FOUND_TIME")
      val sessionCountList = ipRow.getAs[ofRef[AnyRef]]("SESSION_COUNT_LIST")
      val bytesSumList = ipRow.getAs[ofRef[AnyRef]]("BYTES_SUM_LIST")
      val ipTypeList = ipRow.getAs[ofRef[String]]("ip_type_list")
      val linkInfo = ipRow.getAs[String]("common_link_info")
      val sepAttributeTuple = separateAttributeByIpType(ipTypeList, sessionCountList, bytesSumList)

      if (ipDoc != null) {
        updateMaxAttribute(ipDoc, lastFoundTime, "LAST_FOUND_TIME")
        updateSumAttribute(ipDoc, sepAttributeTuple._1, "SERVER_SESSION_COUNT")
        updateSumAttribute(ipDoc, sepAttributeTuple._2, "SERVER_BYTES_SUM")
        updateSumAttribute(ipDoc, sepAttributeTuple._3, "CLIENT_SESSION_COUNT")
        updateSumAttribute(ipDoc, sepAttributeTuple._4, "CLIENT_BYTES_SUM")
        replaceAttribute(ipDoc,linkInfo,"COMMON_LINK_INFO")
      } else {
        ipDoc = new BaseDocument
        ipDoc.setKey(ip)
        ipDoc.addAttribute("IP", ip)
        ipDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime)
        ipDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime)
        ipDoc.addAttribute("SERVER_SESSION_COUNT", sepAttributeTuple._1)
        ipDoc.addAttribute("SERVER_BYTES_SUM", sepAttributeTuple._2)
        ipDoc.addAttribute("CLIENT_SESSION_COUNT", sepAttributeTuple._3)
        ipDoc.addAttribute("CLIENT_BYTES_SUM", sepAttributeTuple._4)
        ipDoc.addAttribute("COMMON_LINK_INFO", "")
      }
    }

    ipDoc
  }

  private def getRelationFqdnLocateIpRow(joinRow: (String, (Option[BaseEdgeDocument], Option[Row]))): BaseEdgeDocument = {

    val fqdnLocIpDocOpt = joinRow._2._1
    var fqdnLocIpDoc = fqdnLocIpDocOpt match {
      case Some(doc) => doc
      case None => null
    }

    val fqdnLocIpRowOpt = joinRow._2._2

    val fqdnLocIpRow = fqdnLocIpRowOpt match {
      case Some(r) => r
      case None => null
    }

    if (fqdnLocIpDoc != null){
      updateProtocolDocument(fqdnLocIpDoc)
    }

    if (fqdnLocIpRow != null){
      val fqdn = fqdnLocIpRow.getAs[String]("FQDN")
      val serverIp = fqdnLocIpRow.getAs[String]("common_server_ip")
      val firstFoundTime = fqdnLocIpRow.getAs[Long]("FIRST_FOUND_TIME")
      val lastFoundTime = fqdnLocIpRow.getAs[Long]("LAST_FOUND_TIME")
      val countTotalList = fqdnLocIpRow.getAs[ofRef[AnyRef]]("COUNT_TOTAL_LIST")
      val schemaTypeList = fqdnLocIpRow.getAs[ofRef[AnyRef]]("schema_type_list")
      val distCipRecent = fqdnLocIpRow.getAs[ofRef[String]]("DIST_CIP_RECENT")

      val sepAttritubeMap: Map[String, Long] = separateAttributeByProtocol(schemaTypeList, countTotalList)
      val distinctIp: Array[String] = mergeDistinctIp(distCipRecent)

      val key = fqdn.concat("-" + serverIp)

      if (fqdnLocIpDoc != null) {
        updateMaxAttribute(fqdnLocIpDoc, lastFoundTime, "LAST_FOUND_TIME")
        updateProtocolAttritube(fqdnLocIpDoc, sepAttritubeMap)
        updateDistinctIp(fqdnLocIpDoc, distinctIp)
      } else {
        fqdnLocIpDoc = new BaseEdgeDocument()
        fqdnLocIpDoc.setKey(key)
        fqdnLocIpDoc.setFrom("FQDN/" + fqdn)
        fqdnLocIpDoc.setTo("IP/" + serverIp)
        fqdnLocIpDoc.addAttribute("FIRST_FOUND_TIME", firstFoundTime)
        fqdnLocIpDoc.addAttribute("LAST_FOUND_TIME", lastFoundTime)
        putProtocolAttritube(fqdnLocIpDoc, sepAttritubeMap)
        putDistinctIp(fqdnLocIpDoc, distinctIp)
      }
    }

    fqdnLocIpDoc
  }

}
