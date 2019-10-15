package com.zgw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/15.
  */
object Sample {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Sample").set("spark.testing.memory", "2147480000")
    //创建spark上下文对象
    val sc = new SparkContext(config)
    val rdd: RDD[String] = sc.makeRDD(Array("hello1","hello1","hello2","hello3","hello4","hello5","hello6","hello1","hello1","hello2","hello3"))
    val sampleRDD: RDD[String] = rdd.sample(false,0.7)
    sampleRDD.foreach(println)
  }

}
