package com.zgw.spark

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by Zhaogw&Lss on 2019/10/16.
  */
object PartitionBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    //val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,3,21,1),4)
    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val partitionBy: RDD[(String, Int)] = listRDD.partitionBy(new Mypari(3))
    partitionBy.saveAsTextFile("output")
  }

}
//声明分区器
class Mypari(partitions: Int) extends Partitioner{
  override def numPartitions: Int = {
    partitions
  }

  override def getPartition(key: Any): Int = {
    1
  }
}