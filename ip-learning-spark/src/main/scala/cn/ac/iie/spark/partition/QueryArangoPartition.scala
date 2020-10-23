package cn.ac.iie.spark.partition

import org.apache.spark.Partition

class QueryArangoPartition(idx: Int, val offset: Long, val separate: Long) extends Partition{
  override def index: Int = idx
}
