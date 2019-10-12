package com.zgw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/12.
  */
object Glom {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("MapPartitions").set("spark.testing.memory","2147480000")
    //创建spark上下文对象
    val sc = new SparkContext(config)
    //glom算子，分组，按照传入函数的返回值进行分组。将相同的key对应的值放入一个迭代器。
    val makeRDD: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6,7,8),3)
    val glomRDD: RDD[Array[Int]] = makeRDD.glom()
    glomRDD.collect().foreach(array=>{
      println(array.mkString(","))
    })
  }

}
