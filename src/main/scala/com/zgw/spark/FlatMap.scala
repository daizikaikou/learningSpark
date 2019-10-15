package com.zgw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/12.
  */
object FlatMap {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("FlatMap").set("spark.testing.memory","2147480000")
    //创建spark上下文对象
    val sc = new SparkContext(config)
    //(Array(1,2),(Array(2,3)===>1,2,2,3
    val listRdd: RDD[Array[Int]] = sc.makeRDD(List(Array(1,2),(Array(2,3))))
    val mapInfo: RDD[Int] = listRdd.flatMap(datas=>datas)
    mapInfo.collect().foreach(println)
  }

}
