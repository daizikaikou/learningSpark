package com.zgw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/12.
  */
object MapPartitionWithIndex {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("MapPartitionWithIndex").set("spark.testing.memory","2147480000")
    //创建spark上下文对象
    val sc = new SparkContext(config)
  //创建rdd，使每个元素跟所在的分区形成一个元组
    val listRdd: RDD[Int] = sc.makeRDD(1 to 10,2)
    val tupRDD: RDD[(Int, String)] = listRdd.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((_, "分区号:" + num))
      }
    }

    tupRDD.collect().foreach(println)
  }

}
