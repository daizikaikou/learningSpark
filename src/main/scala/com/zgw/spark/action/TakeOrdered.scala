package com.zgw.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/28.
  */
object TakeOrdered {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val makeRDD: RDD[Int] = sc.makeRDD(Array(2,5,4,6,3,8))
    val ordered: Array[Int] = makeRDD.takeOrdered(3)
    ordered.foreach(println)
  }

}
