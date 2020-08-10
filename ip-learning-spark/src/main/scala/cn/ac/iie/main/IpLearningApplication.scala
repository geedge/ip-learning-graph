package cn.ac.iie.main

import cn.ac.iie.service.update.UpdateDocument

object IpLearningApplication {

  def main(args: Array[String]): Unit = {
    UpdateDocument.update()
  }
}
