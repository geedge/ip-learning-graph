package cn.ac.iie.dao

import cn.ac.iie.test.ArangoDbTest.arangoDB
import cn.ac.iie.test.Config
import com.arangodb.entity.{BaseDocument, BaseEdgeDocument}
import org.apache.spark.sql.DataFrame

import scala.util.Try

object UpdateArangoGraph {

  /**
    * 更新FQDN点
    */
  def updateFQDNVertex(v_FQDN_DF:DataFrame): Unit ={

    v_FQDN_DF.printSchema()
    v_FQDN_DF.foreachPartition(iter => {
      val v_FQDN_Coll = arangoDB.db("insert_iplearn_index").collection("V_FQDN")
      val docs_Insert = new java.util.ArrayList[BaseDocument]()
      val docs_Update = new java.util.ArrayList[BaseDocument]()
      var i = 0
      iter.foreach(row => {
        val fqdn = row.getAs[String]("FQDN_NAME")
        val v_Fqdn_First = row.getAs[Long]("FQDN_FIRST_FOUND_TIME")
        val v_Fqdn_Last = row.getAs[Long]("FQDN_LAST_FOUND_TIME")
        val v_Fqdn_Cnt = row.getAs[Long]("FQDN_COUNT_TOTAL")

        if (v_FQDN_Coll.documentExists(fqdn)) {
          val document: BaseDocument = v_FQDN_Coll.getDocument(fqdn, classOf[BaseDocument])
          val fqdn_Cnt = Try(document.getAttribute("FQDN_COUNT_TOTAL")).getOrElse(0).toString.toInt
          document.addAttribute("FQDN_COUNT_TOTAL", fqdn_Cnt)
          document.addAttribute("FQDN_LAST_FOUND_TIME", v_Fqdn_Last)
          docs_Update.add(document)
        } else {
          val baseDocument: BaseDocument = new BaseDocument()
          baseDocument.setKey(fqdn)
          baseDocument.addAttribute("FQDN_NAME", fqdn)
          baseDocument.addAttribute("FQDN_FIRST_FOUND_TIME", v_Fqdn_First)
          baseDocument.addAttribute("FQDN_LAST_FOUND_TIME", v_Fqdn_Last)
          baseDocument.addAttribute("FQDN_COUNT_TOTAL", v_Fqdn_Cnt)
          docs_Insert.add(baseDocument)
        }
        i+=1
      })
      Try(v_FQDN_Coll.importDocuments(docs_Insert))
      Try(v_FQDN_Coll.updateDocuments(docs_Update))
    })
  }

  /**
    * 更新IP点
    */
  def updateIPVertex(v_IP_DF:DataFrame): Unit ={
    v_IP_DF.printSchema()
    v_IP_DF.foreachPartition(iter => {
      val v_IP_Coll = arangoDB.db("insert_iplearn_index").collection("V_IP")
      val docs_Insert: java.util.ArrayList[BaseDocument] = new java.util.ArrayList[BaseDocument]()
      val docs_Update: java.util.ArrayList[BaseDocument] = new java.util.ArrayList[BaseDocument]()
      var i = 0

      iter.foreach(row => {
        val ip = row.getAs[String]("IP")
        val location = row.getAs[String]("location")
        val v_IP_First = row.getAs[Long]("IP_FIRST_FOUND_TIME")
        val v_IP_Last = row.getAs[Long]("IP_LAST_FOUND_TIME")
        val v_IP_Cnt = row.getAs[Long]("IP_COUNT_TOTAL")

        if (v_IP_Coll.documentExists(ip)) {
          val document: BaseDocument = v_IP_Coll.getDocument(ip, classOf[BaseDocument])
          val ip_Cnt = Try(document.getAttribute("IP_APPEAR_COUNT")).getOrElse(0).toString.toInt
          document.addAttribute("LAST_FOUND_TIME", v_IP_Last)
          document.addAttribute("IP_APPEAR_COUNT", v_IP_Cnt+ip_Cnt)
          docs_Update.add(document)
        } else {
          val baseDocument: BaseDocument = new BaseDocument()
          baseDocument.setKey(ip)
          baseDocument.addAttribute("IP", ip)
          baseDocument.addAttribute("IP_LOCATION", location)
          baseDocument.addAttribute("FIRST_FOUND_TIME", v_IP_First)
          baseDocument.addAttribute("LAST_FOUND_TIME", v_IP_Last)
          baseDocument.addAttribute("IP_APPEAR_COUNT", v_IP_Cnt)
          docs_Insert.add(baseDocument)
        }
        i+=1
      })
      Try(v_IP_Coll.importDocuments(docs_Insert))
      Try(v_IP_Coll.updateDocuments(docs_Update))
    })

  }

  /**
    * 统计e_Address_Fqdn_to_IP
    */
  def updateFQDNAddressIPEdge(e_Address_v_FQDN_to_v_IP_DF:DataFrame): Unit ={
    e_Address_v_FQDN_to_v_IP_DF.printSchema()
    e_Address_v_FQDN_to_v_IP_DF.foreachPartition(iter => {
      val e_Add_Fqdn_to_IP_Coll = arangoDB.db("insert_iplearn_index").collection("E_ADDRESS_V_FQDN_TO_V_IP")
      val docs_Insert: java.util.ArrayList[BaseEdgeDocument] = new java.util.ArrayList[BaseEdgeDocument]()
      val docs_Update: java.util.ArrayList[BaseEdgeDocument] = new java.util.ArrayList[BaseEdgeDocument]()
      var i = 0
      iter.foreach(row => {
        val fqdn = row.getAs[String]("V_FQDN")
        val ip = row.getAs[String]("V_IP")
        val e_First = row.getAs[Long]("FIRST_FOUND_TIME")
        val e_Last = row.getAs[Long]("LAST_FOUND_TIME")
        val e_Cnt = row.getAs[Long]("COUNT_TOTAL")

        if (e_Add_Fqdn_to_IP_Coll.documentExists(fqdn+"-"+ip)) {
          val document: BaseEdgeDocument = e_Add_Fqdn_to_IP_Coll.getDocument(fqdn+"-"+ip, classOf[BaseEdgeDocument])
          val e_new_Cnt = Try(document.getAttribute("COUNT_TOTAL")).getOrElse(0).toString.toInt
          document.addAttribute("LAST_FOUND_TIME", e_Last)
          document.addAttribute("COUNT_TOTAL", e_new_Cnt+e_Cnt)
          docs_Update.add(document)
        } else {
          val baseDocument: BaseEdgeDocument = new BaseEdgeDocument()
          baseDocument.setKey(fqdn+"-"+ip)
          baseDocument.setFrom(s"V_FQDN/$fqdn")
          baseDocument.setTo(s"V_IP/$ip")
          baseDocument.addAttribute("COUNT_TOTAL",e_Cnt)
          baseDocument.addAttribute("FIRST_FOUND_TIME",e_First)
          baseDocument.addAttribute("LAST_FOUND_TIME",e_Last)
          docs_Insert.add(baseDocument)
        }
        //        println(fqdn+"-"+ip)
        i+=1
      })
      Try(e_Add_Fqdn_to_IP_Coll.importDocuments(docs_Insert))
      Try(e_Add_Fqdn_to_IP_Coll.updateDocuments(docs_Update))
    })
  }

  /**
    * 统计e_Visit_v_IP_to_v_FQDN
    */
  def updateIPVisitFQDNEdge(e_Visit_v_IP_to_v_FQDN_DF:DataFrame): Unit ={
    e_Visit_v_IP_to_v_FQDN_DF.printSchema()
    e_Visit_v_IP_to_v_FQDN_DF.foreachPartition(iter => {
      val e_Visit_Fqdn_to_IP_Coll = arangoDB.db("insert_iplearn_index").collection("E_VISIT_V_IP_TO_V_FQDN")
      val docs_Insert: java.util.ArrayList[BaseEdgeDocument] = new java.util.ArrayList[BaseEdgeDocument]()
      val docs_Update: java.util.ArrayList[BaseEdgeDocument] = new java.util.ArrayList[BaseEdgeDocument]()
      var i = 0
      iter.foreach(row => {
        val fqdn = row.getAs[String]("V_FQDN")
        val ip = row.getAs[String]("V_IP")
        val e_First = row.getAs[Long]("FIRST_FOUND_TIME")
        val e_Last = row.getAs[Long]("LAST_FOUND_TIME")
        val e_Cnt = row.getAs[Long]("COUNT_TOTAL")

        if (e_Visit_Fqdn_to_IP_Coll.documentExists(ip+"-"+fqdn)) {
          val document: BaseEdgeDocument = e_Visit_Fqdn_to_IP_Coll.getDocument(ip+"-"+fqdn, classOf[BaseEdgeDocument])
          val e_new_Cnt = Try(document.getAttribute("COUNT_TOTAL")).getOrElse(0).toString.toInt
          document.addAttribute("LAST_FOUND_TIME", e_Last)
          document.addAttribute("COUNT_TOTAL", e_new_Cnt+e_Cnt)
          docs_Update.add(document)
        } else {
          val baseDocument: BaseEdgeDocument = new BaseEdgeDocument()
          baseDocument.setKey(ip+"-"+fqdn)
          baseDocument.setFrom(s"V_IP/$ip")
          baseDocument.setTo(s"V_FQDN/$fqdn")
          baseDocument.addAttribute("COUNT_TOTAL",e_Cnt)
          baseDocument.addAttribute("FIRST_FOUND_TIME",e_First)
          baseDocument.addAttribute("LAST_FOUND_TIME",e_Last)
          docs_Insert.add(baseDocument)
        }
        i+=1
      })
      Try(e_Visit_Fqdn_to_IP_Coll.importDocuments(docs_Insert))
      Try(e_Visit_Fqdn_to_IP_Coll.updateDocuments(docs_Update))
    })


  }


}
