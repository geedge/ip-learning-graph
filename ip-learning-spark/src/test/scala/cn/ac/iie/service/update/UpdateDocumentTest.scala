package cn.ac.iie.service.update

import java.util
import java.util.ArrayList
import java.util.concurrent.ConcurrentHashMap

import cn.ac.iie.dao.BaseArangoData
import cn.ac.iie.dao.BaseArangoData._
import com.arangodb.entity.{BaseDocument, BaseEdgeDocument}

import scala.collection.mutable.WrappedArray.ofRef

object UpdateDocumentTest {
  def main(args: Array[String]): Unit = {
    val baseArangoData = new BaseArangoData()
    baseArangoData.readHistoryData("R_LOCATE_FQDN2IP", historyRelationFqdnAddressIpMap, classOf[BaseEdgeDocument])

    val value = BaseArangoData.historyRelationFqdnAddressIpMap.keys()
    while (value.hasMoreElements) {
      val integer: Integer = value.nextElement()
      val map: ConcurrentHashMap[String, BaseEdgeDocument] = historyRelationFqdnAddressIpMap.get(integer)
      val unit = map.keys()
      while (unit.hasMoreElements) {
        val key = unit.nextElement()
        val edgeDocument = map.get(key)
        //        val longs = edgeDocument.getAttribute("DNS_CNT_RECENT").asInstanceOf[util.ArrayList[Long]]
        //        val strings = edgeDocument.getAttribute("DIST_CIP").asInstanceOf[util.ArrayList[String]]
        val strings = edgeDocument.getAttribute("DIST_CIP").asInstanceOf[Array[String]]
        val longs = edgeDocument.getAttribute("DNS_CNT_RECENT").asInstanceOf[Array[java.lang.Long]]
        println(longs.toString + "---" + strings.toString)
      }
    }
  }

}
