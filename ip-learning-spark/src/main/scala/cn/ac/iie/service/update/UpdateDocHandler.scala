package cn.ac.iie.service.update


import java.lang

import cn.ac.iie.config.ApplicationConfig
import cn.ac.iie.service.read.ReadHistoryArangoData
import com.arangodb.entity.{BaseDocument, BaseEdgeDocument}

import scala.collection.mutable
import scala.collection.mutable.WrappedArray.ofRef

object UpdateDocHandler {
  val PROTOCOL_SET: Set[String] = Set("HTTP","TLS","DNS")

  def updateMaxAttribute(hisDoc: BaseDocument,newAttribute:Long,attributeName:String): Unit ={
    var hisAttritube = hisDoc.getAttribute(attributeName).toString.toLong
    if (newAttribute > hisAttritube){
      hisAttritube = newAttribute
    }
    hisDoc.addAttribute(attributeName,hisAttritube)
  }

  def updateSumAttribute(hisDoc: BaseDocument,newAttribute:Long,attributeName:String): Unit ={
    val hisAttritube = hisDoc.getAttribute(attributeName).toString.toLong
    hisDoc.addAttribute(attributeName,newAttribute+hisAttritube)
  }

  def separateAttributeByIpType(ipTypeList:ofRef[String],
                                sessionCountList:ofRef[AnyRef],
                                bytesSumList:ofRef[AnyRef]): (Long,Long,Long,Long) ={
    var serverSessionCount = 0L
    var serverBytesSum = 0L
    var clientSessionCount = 0L
    var clientBytesSum = 0L
    if (ipTypeList.length == sessionCountList.length && ipTypeList.length == bytesSumList.length){
      sessionCountList.zip(bytesSumList).zip(ipTypeList).foreach(t => {
        t._2 match {
          case "server" =>
            serverSessionCount = t._1._1.toString.toLong
            serverBytesSum = t._1._2.toString.toLong
          case "client" =>
            clientSessionCount = t._1._1.toString.toLong
            clientBytesSum = t._1._2.toString.toLong
        }
      })
    }
    (serverSessionCount, serverBytesSum, clientSessionCount, clientBytesSum)
  }

  def separateAttributeByProtocol(schemaTypeList:ofRef[AnyRef],countTotalList:ofRef[AnyRef]): Map[String, Long] ={
    var protocolMap: Map[String, Long] = Map()
    if (schemaTypeList.length == countTotalList.length){
      protocolMap = schemaTypeList.zip(countTotalList).map(t => (t._1.toString,t._2.toString.toLong)).toMap
    }
    PROTOCOL_SET.foreach(protocol => {
      if (!protocolMap.contains(protocol)){
        protocolMap += (protocol -> 0L)
      }
    })
    protocolMap
  }

  def updateProtocolAttritube(hisDoc:BaseEdgeDocument, protocolMap: Map[String, Long]): Unit ={
    var protocolType = hisDoc.getAttribute("PROTOCOL_TYPE").toString
    protocolMap.foreach(t => {
      if (t._2 > 0 && !protocolType.contains(t._1)){
        protocolType = protocolType.concat(","+ t._1)
      }
      val cntTotalName = t._1.concat("_CNT_TOTAL")
      val cntRecentName = t._1.concat("_CNT_RECENT")
      val cntRecent: Array[lang.Long] = hisDoc.getAttribute(cntRecentName).asInstanceOf[Array[java.lang.Long]]
      cntRecent.update(0,t._2)
      updateSumAttribute(hisDoc,t._2,cntTotalName)
      hisDoc.addAttribute(cntRecentName,cntRecent)
    })
    hisDoc.addAttribute("PROTOCOL_TYPE",protocolType)
  }

  def putProtocolAttritube(doc:BaseEdgeDocument, protocolMap: Map[String, Long]): Unit ={
    val protocolTypeBuilder = new mutable.StringBuilder()
    protocolMap.foreach(t => {
      if (t._2 > 0){
        protocolTypeBuilder.append(","+t._1)
      }
      val cntTotalName = t._1.concat("_CNT_TOTAL")
      val cntRecentName = t._1.concat("_CNT_RECENT")
      val cntRecent: Array[Long] = new Array[Long](ApplicationConfig.RECENT_COUNT_HOUR)
      cntRecent.update(0,t._2)
      doc.addAttribute(cntTotalName,t._2)
      doc.addAttribute(cntRecentName,cntRecent)
    })
    doc.addAttribute("PROTOCOL_TYPE",protocolTypeBuilder.toString().replaceFirst(",",""))
  }

  def mergeDistinctIp(distCipRecent:ofRef[ofRef[String]]): Array[String] ={
     distCipRecent.flatten.distinct.take(ApplicationConfig.DISTINCT_CLIENT_IP_NUM).toArray
  }

  def putDistinctIp(doc:BaseEdgeDocument,newDistinctIp:Array[String]): Unit ={
    val map = newDistinctIp.map(ip => {
      (ip, ReadHistoryArangoData.currentHour)
    }).toMap
    doc.addAttribute("DIST_CIP",map.keys.toArray)
    doc.addAttribute("DIST_CIP_TS",map.values.toArray)
  }

  def updateDistinctIp(hisDoc:BaseEdgeDocument,newDistinctIp:Array[String]): Unit ={
    val hisDistCip = hisDoc.getAttribute("DIST_CIP").asInstanceOf[Array[String]]
    val hisDistCipTs = hisDoc.getAttribute("DIST_CIP_TS").asInstanceOf[Array[Long]]
    if (hisDistCip.length == hisDistCipTs.length){
      val distCipToTsMap: Map[String, Long] = hisDistCip.zip(hisDistCipTs).toMap
      val muDistCipToTsMap: mutable.Map[String, Long] = mutable.Map(distCipToTsMap.toSeq:_*)
      newDistinctIp.foreach(cip => {
        muDistCipToTsMap.put(cip,ReadHistoryArangoData.currentHour)
      })
      val resultMap = muDistCipToTsMap.toList.sortBy(-_._2).take(ApplicationConfig.DISTINCT_CLIENT_IP_NUM).toMap
      hisDoc.addAttribute("DIST_CIP",resultMap.keys.toArray)
      hisDoc.addAttribute("DIST_CIP_TS",resultMap.values.toArray)
    }
  }

}
