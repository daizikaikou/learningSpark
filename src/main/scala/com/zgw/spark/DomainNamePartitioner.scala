package com.zgw.spark

import org.apache.spark.Partitioner

/**
  * Created by Zhaogw&Lss on 2019/10/29.
  * 自定义分区
  */
class DomainNamePartitioner(numParts: Int) extends Partitioner{
  //返回创建出来的分区数。
  override def numPartitions: Int = numParts
  //返回给定键的分区编号
  override def getPartition(key: Any): Int ={
    val domain = new java.net.URL(key.toString).getHost()
    val code = (domain.hashCode % numPartitions)
    if(code < 0) {
      code + numPartitions // 使其非负
    }else{
      code
    }
  }
  // 用来让Spark区分分区函数对象的Java equals方法
  override def equals(other: Any): Boolean = other match {
    case dnp: DomainNamePartitioner =>
      dnp.numPartitions == numPartitions
    case _ =>
      false
  }

}
