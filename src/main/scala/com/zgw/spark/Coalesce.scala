package com.zgw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/15.
  */
object Coalesce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val makeRdd: RDD[Int] = sc.makeRDD(1 to 16,4)
    //缩减分区数
    val coalesceRdd: RDD[Int] = makeRdd.coalesce(3)
    coalesceRdd.saveAsTextFile("output")
    println(coalesceRdd.partitions.size)
  }

}
