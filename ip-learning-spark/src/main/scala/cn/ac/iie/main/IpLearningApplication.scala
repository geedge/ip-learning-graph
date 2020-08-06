package cn.ac.iie.main

import cn.ac.iie.service.update.UpdateDocument._
import cn.ac.iie.utils.{ExecutorThreadPool, SparkSessionUtil}

object IpLearningApplication {
  private val pool = ExecutorThreadPool.getInstance

  def main(args: Array[String]): Unit = {
    try {
      updateVertexFqdn()
      updateVertexIp()
      updateRelationFqdnLocateIp()
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      pool.shutdown()
      arangoManger.clean()
      SparkSessionUtil.closeSpark()
    }
  }
}
