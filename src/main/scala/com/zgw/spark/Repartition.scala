package com.zgw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/15.
  */
object Repartition {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Distinct").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val parallelize: RDD[Int] = sc.parallelize(1 to 16,4)
    println(parallelize.partitions.size)
    val rerdd = parallelize.repartition(2)
    println(rerdd.partitions.size)
    val glom: RDD[Array[Int]] = rerdd.glom()
    glom.collect().foreach(array=>{
      println(array.mkString(","))
    })

  }

}
