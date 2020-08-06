package cn.ac.iie.service.partition

import org.apache.spark.Partitioner

class CustomPartitioner(numPartition: Int) extends Partitioner{
  override def numPartitions: Int = numPartition

  override def getPartition(key: Any): Int = {
    Math.abs(key.hashCode()) % numPartition
  }
}
