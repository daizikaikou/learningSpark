package com.zgw.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by Zhaogw&Lss on 2019/10/12.
  */
object GlomForMax {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("MapPartitions").set("spark.testing.memory","2147480000")
    //创建spark上下文对象
    val sc = new SparkContext(config)
    //glom算子，求最大值
    val makeRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8),3)
    val glomRDD: RDD[Array[Int]] = makeRDD.glom()
    glomRDD.collect().foreach(array1=>{
      println(array1.mkString(","))
    })
    println("****")
    glomRDD.collect().foreach(array=>{
      println(array.max)

    })


  }

}
