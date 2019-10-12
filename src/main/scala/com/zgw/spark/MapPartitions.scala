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
    /*创建一个1-10数组的RDD，将所有元素*2形成新的RDD*/
    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)
    listRdd.collect.foreach(println)

    val mapParRDD: RDD[Nothing] = listRdd.mapPartitions(a=>a)


  }

}
