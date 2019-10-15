package com.zgw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/15.
  */
object SortBy {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("MapPartitions").set("spark.testing.memory", "2147480000")
    //创建spark上下文对象
    val sc = new SparkContext(config)
    val rdd = sc.parallelize(List(2, 1, 3, 4))
    val sortByRDD: RDD[Int] = rdd.sortBy(x => x)
    sortByRDD.collect().foreach(println)
  }
}
