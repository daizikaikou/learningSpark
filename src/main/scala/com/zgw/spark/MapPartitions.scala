package com.zgw.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by Zhaogw&Lss on 2019/10/12.
  */
object MapPartitions {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("MapPartitions").set("spark.testing.memory","2147480000")
    //创建spark上下文对象
    val sc = new SparkContext(config)
    /*创建一个1-10数组的RDD*/
    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)
    //mappartition对一个rdd中所有的分区进行遍历
    //优于map算子，减少发到执行器的交互次数
    //可能内存溢出
    val partitions: RDD[Int] = listRdd.mapPartitions(data=>{data.map(data=>data*2)})
    partitions.collect.foreach(println)


  }

}
