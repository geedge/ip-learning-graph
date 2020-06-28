package cn.ac.iie.dao

import cn.ac.iie.etl.CursorTransform
import cn.ac.iie.pojo.{BaseEdgeIPVisitFqdn, BaseEgdeFqdnAddressIP, BaseVertexFqdn, BaseVertexIP}
import cn.ac.iie.test.Config
import cn.ac.iie.utils.{ConfigUtils, InitArangoDBPool}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object UpdateArangoGraphByArangoSpark {
  /**
    * 更新FQDN点
    */
  def updateFQDNVertex(v_FQDN_DF:DataFrame,v_FQDN_Cursor_DF: DataFrame): Unit ={
    v_FQDN_DF.printSchema()
    v_FQDN_Cursor_DF.printSchema()

    val v_Fqdn_Join_Df = v_FQDN_DF
      .join(v_FQDN_Cursor_DF,v_FQDN_DF("new_fqdn_name")===v_FQDN_Cursor_DF("key"),"fullouter")
    v_Fqdn_Join_Df.printSchema()

    v_Fqdn_Join_Df.coalesce(Config.REPARTITION_NUMBER).foreachPartition(iter => {
      val v_FQDN_Coll = InitArangoDBPool.arangoDB.db(ConfigUtils.ARANGODB_DB_NAME).collection("V_FQDN")
      val docs_Replace = new java.util.ArrayList[BaseVertexFqdn]()
      val docs_Insert = new java.util.ArrayList[BaseVertexFqdn]()

      iter.foreach(row => {
        val new_fqdn_name = row.getAs[String]("new_fqdn_name")
        val new_fqdn_first_found_time = row.getAs[Long]("new_fqdn_first_found_time")
        val new_fqdn_last_found_time = row.getAs[Long]("new_fqdn_last_found_time")
        val new_fqdn_count_total = row.getAs[Long]("new_fqdn_count_total")

        val fqdn = row.getAs[String]("key")
        val v_Fqdn_First = row.getAs[Long]("FQDN_FIRST_FOUND_TIME")
        val v_Fqdn_Cnt = row.getAs[Long]("FQDN_COUNT_TOTAL")

        if (fqdn != null)  {
          val document: BaseVertexFqdn = new BaseVertexFqdn()
          document.setKey(new_fqdn_name)
          document.setFQDN_NAME(new_fqdn_name)
          document.setFQDN_FIRST_FOUND_TIME(v_Fqdn_First)
          document.setFQDN_LAST_FOUND_TIME(new_fqdn_last_found_time)
          document.setFQDN_COUNT_TOTAL(v_Fqdn_Cnt+new_fqdn_count_total)
          docs_Replace.add(document)
        } else {
          val baseDocument: BaseVertexFqdn = new BaseVertexFqdn()
          baseDocument.setKey(new_fqdn_name)
          baseDocument.setFQDN_NAME(new_fqdn_name)
          baseDocument.setFQDN_FIRST_FOUND_TIME(new_fqdn_first_found_time)
          baseDocument.setFQDN_LAST_FOUND_TIME(new_fqdn_last_found_time)
          baseDocument.setFQDN_COUNT_TOTAL(new_fqdn_count_total)
          docs_Insert.add(baseDocument)
        }
      })
      Try(v_FQDN_Coll.replaceDocuments(docs_Replace))
      Try(v_FQDN_Coll.importDocuments(docs_Insert))
    })

  }

  /**
    * 更新IP点
    */
  def updateIPVertex(v_IP_DF:DataFrame,v_IP_Cursor_DF: DataFrame): Unit ={
    v_IP_DF.printSchema()

    v_IP_Cursor_DF.printSchema()

    val v_IP_Join_DF = v_IP_DF.join(v_IP_Cursor_DF,v_IP_DF("new_ip")===v_IP_Cursor_DF("key"),"fullouter")
    v_IP_Join_DF.printSchema()

    v_IP_Join_DF.coalesce(Config.REPARTITION_NUMBER).foreachPartition(iter => {
      val v_IP_Coll = InitArangoDBPool.arangoDB.db(ConfigUtils.ARANGODB_DB_NAME).collection("V_IP")
      val docs_Insert: java.util.ArrayList[BaseVertexIP] = new java.util.ArrayList[BaseVertexIP]()
      val docs_Replace: java.util.ArrayList[BaseVertexIP] = new java.util.ArrayList[BaseVertexIP]()

      iter.foreach(row => {
        val new_Ip = row.getAs[String]("new_ip")
        val new_Location = row.getAs[String]("new_location")
        val new_Ip_First_Found_Time = row.getAs[Long]("new_ip_first_found_time")
        val new_Ip_Last_Found_Time = row.getAs[Long]("new_ip_last_found_time")
        val new_Ip_Count_Total = row.getAs[Long]("new_ip_count_total")

        val key = row.getAs[String]("key")
        val location = row.getAs[String]("IP_LOCATION")
        val v_IP_First = row.getAs[Long]("FIRST_FOUND_TIME")
        val v_IP_Cnt = row.getAs[Long]("IP_APPEAR_COUNT")


        if (key != null) {
          val document = new BaseVertexIP()
          document.setKey(key)
          document.setIP(key)
          document.setLAST_FOUND_TIME(new_Ip_Last_Found_Time)
          document.setIP_APPEAR_COUNT(v_IP_Cnt+new_Ip_Count_Total)
          document.setFIRST_FOUND_TIME(v_IP_First)
          document.setIP_LOCATION(location)
          docs_Replace.add(document)
        } else {
          val baseDocument = new BaseVertexIP()
          baseDocument.setKey(new_Ip)
          baseDocument.setIP(new_Ip)
          baseDocument.setLAST_FOUND_TIME(new_Ip_Last_Found_Time)
          baseDocument.setIP_APPEAR_COUNT(new_Ip_Count_Total)
          baseDocument.setFIRST_FOUND_TIME(new_Ip_First_Found_Time)
          baseDocument.setIP_LOCATION(new_Location)
          docs_Insert.add(baseDocument)
        }
      })
      Try(v_IP_Coll.importDocuments(docs_Insert))
      Try(v_IP_Coll.updateDocuments(docs_Replace))
    })

  }

  /**
    * 统计e_Address_Fqdn_to_IP
    */
  def updateFQDNAddressIPEdge(e_Address_v_FQDN_to_v_IP_DF:DataFrame,e_Fqdn_Address_IP_Cursor_DF: DataFrame): Unit ={
    e_Address_v_FQDN_to_v_IP_DF.printSchema()
    e_Fqdn_Address_IP_Cursor_DF.printSchema()

    e_Fqdn_Address_IP_Cursor_DF.printSchema()

    val e_Address_v_FQDN_to_v_IP_Join_DF = e_Address_v_FQDN_to_v_IP_DF
      .join(e_Fqdn_Address_IP_Cursor_DF,
        e_Address_v_FQDN_to_v_IP_DF("new_key")===e_Fqdn_Address_IP_Cursor_DF("key"),
        "fullouter")

    e_Address_v_FQDN_to_v_IP_Join_DF.printSchema()

    e_Address_v_FQDN_to_v_IP_Join_DF.coalesce(Config.REPARTITION_NUMBER).foreachPartition(iter => {
      val e_Add_Fqdn_to_IP_Coll = InitArangoDBPool.arangoDB.db(ConfigUtils.ARANGODB_DB_NAME).collection("E_ADDRESS_V_FQDN_TO_V_IP")
      val docs_Insert: java.util.ArrayList[BaseEgdeFqdnAddressIP] = new java.util.ArrayList[BaseEgdeFqdnAddressIP]()
      val docs_Replace: java.util.ArrayList[BaseEgdeFqdnAddressIP] = new java.util.ArrayList[BaseEgdeFqdnAddressIP]()
      iter.foreach(row => {
        val new_Fqdn = row.getAs[String]("new_fqdn")
        val new_IP = row.getAs[String]("new_ip")
        val new_Key = row.getAs[String]("new_key")
        val new_First_Found_Time = row.getAs[Long]("new_first_found_time")
        val new_Last_Found_Time = row.getAs[Long]("new_last_found_time")
        val new_Count_Total = row.getAs[Long]("new_count_total")

        val from = row.getAs[String]("from")
        val to = row.getAs[String]("to")
        val key = row.getAs[String]("key")
        val e_First_Time = row.getAs[Long]("FIRST_FOUND_TIME")
        val e_Count_Total = row.getAs[Long]("COUNT_TOTAL")

        if (key != null) {
          val document = new BaseEgdeFqdnAddressIP()
          document.setKey(key)
          document.setFrom(from)
          document.setTo(to)
          document.setLAST_FOUND_TIME(new_Last_Found_Time)
          document.setFIRST_FOUND_TIME(e_First_Time)
          document.setCOUNT_TOTAL(new_Count_Total+e_Count_Total)
          docs_Replace.add(document)
        } else {
          val baseDocument: BaseEgdeFqdnAddressIP = new BaseEgdeFqdnAddressIP()
          baseDocument.setKey(new_Key)
          baseDocument.setFrom(s"V_FQDN/$new_Fqdn")
          baseDocument.setTo(s"V_IP/$new_IP")
          baseDocument.setLAST_FOUND_TIME(new_Last_Found_Time)
          baseDocument.setFIRST_FOUND_TIME(new_First_Found_Time)
          baseDocument.setCOUNT_TOTAL(new_Count_Total)
          docs_Insert.add(baseDocument)
        }
      })
      Try(e_Add_Fqdn_to_IP_Coll.importDocuments(docs_Insert))
      Try(e_Add_Fqdn_to_IP_Coll.replaceDocuments(docs_Replace))
    })

  }


  /**
    * 统计e_Visit_v_IP_to_v_FQDN
    */
  def updateIPVisitFQDNEdge(e_Visit_v_IP_to_v_FQDN_DF:DataFrame,e_IP_Visit_FQDN_Cursor_DF: DataFrame): Unit = {
    e_Visit_v_IP_to_v_FQDN_DF.printSchema()
    e_IP_Visit_FQDN_Cursor_DF.printSchema()

    e_IP_Visit_FQDN_Cursor_DF.printSchema()

    val e_Visit_v_IP_to_v_FQDN_Join_DF = e_Visit_v_IP_to_v_FQDN_DF
      .join(e_IP_Visit_FQDN_Cursor_DF, e_Visit_v_IP_to_v_FQDN_DF("new_key") === e_IP_Visit_FQDN_Cursor_DF("key"), "fullouter")

    e_Visit_v_IP_to_v_FQDN_Join_DF.printSchema()

    e_Visit_v_IP_to_v_FQDN_Join_DF.coalesce(Config.REPARTITION_NUMBER).foreachPartition(iter => {
      val e_Visit_Fqdn_to_IP_Coll = InitArangoDBPool.arangoDB.db(ConfigUtils.ARANGODB_DB_NAME).collection("E_VISIT_V_IP_TO_V_FQDN")
      val docs_Insert: java.util.ArrayList[BaseEdgeIPVisitFqdn] = new java.util.ArrayList[BaseEdgeIPVisitFqdn]()
      val docs_Replace: java.util.ArrayList[BaseEdgeIPVisitFqdn] = new java.util.ArrayList[BaseEdgeIPVisitFqdn]()
      iter.foreach(row => {
        val new_Fqdn = row.getAs[String]("new_fqdn")
        val new_IP = row.getAs[String]("new_ip")
        val new_Key = row.getAs[String]("new_key")
        val new_First_Found_Time = row.getAs[Long]("new_first_found_time")
        val new_Last_Found_Time = row.getAs[Long]("new_last_found_time")
        val new_Count_Total = row.getAs[Long]("new_count_total")

        val to = row.getAs[String]("to")
        val from = row.getAs[String]("from")
        val key = row.getAs[String]("key")
        val e_First_Time = row.getAs[Long]("FIRST_FOUND_TIME")
        val e_Count_Total = row.getAs[Long]("COUNT_TOTAL")

        if (key != null) {
          val document = new BaseEdgeIPVisitFqdn()
          document.setKey(key)
          document.setFrom(from)
          document.setTo(to)
          document.setLAST_FOUND_TIME(new_Last_Found_Time)
          document.setFIRST_FOUND_TIME(e_First_Time)
          document.setCOUNT_TOTAL(new_Count_Total+e_Count_Total)
          docs_Replace.add(document)
        } else {
          val baseDocument: BaseEdgeIPVisitFqdn = new BaseEdgeIPVisitFqdn()
          baseDocument.setKey(new_Key)
          baseDocument.setFrom(s"V_FQDN/$new_Fqdn")
          baseDocument.setTo(s"V_IP/$new_IP")
          baseDocument.setLAST_FOUND_TIME(new_Last_Found_Time)
          baseDocument.setFIRST_FOUND_TIME(new_First_Found_Time)
          baseDocument.setCOUNT_TOTAL(new_Count_Total)
          docs_Insert.add(baseDocument)
        }
      })
      Try(e_Visit_Fqdn_to_IP_Coll.importDocuments(docs_Insert))
      Try(e_Visit_Fqdn_to_IP_Coll.replaceDocuments(docs_Replace))
    })

  }


}
