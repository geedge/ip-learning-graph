package cn.ac.iie.spark

import cn.ac.iie.spark.rdd.ReadOptions
import cn.ac.iie.utils.SparkSessionUtil
import com.arangodb.entity.BaseDocument
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object RDDTest {
  def main(args: Array[String]): Unit = {

    val sparkContext = SparkSessionUtil.spark.sparkContext

    println(sparkContext.getConf.get("arangodb.hosts"))

    //    val options = ReadOptions("iplearn_media_domain").copy(collection = "R_LOCATE_FQDN2IP")
    val options = ReadOptions("ip-learning-test-0")

    val ipOptions = options.copy(collection = "IP")

    val rdd = ArangoSpark.load[BaseDocument](sparkContext,"IP",options)

    println(rdd.count())
    println(rdd.getNumPartitions)

    val value: RDD[BaseDocument] = rdd.filter(doc => doc.getAttribute("CLIENT_SESSION_COUNT").asInstanceOf[Long] > 100).map(doc => {
      doc.addAttribute("abc", 1)
      doc
    })

    value.map(doc => {(doc.getKey,doc)})

    value.persist(StorageLevel.MEMORY_AND_DISK)

    value.foreach(row => println(row.toString))
    println(value.count())

    SparkSessionUtil.spark.close()
    System.exit(0)

  }

}
