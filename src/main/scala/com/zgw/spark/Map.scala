package com.zgw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/12.
  */
object Map {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Map").set("spark.testing.memory","2147480000")
    //创建spark上下文对象
    val sc = new SparkContext(config)
    /*创建一个1-10数组的RDD，将所有元素*2形成新的RDD*/
    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)
    val mapRDD: RDD[Int] = listRdd.map(x => x*2)

    /*拼接*/
    val data = Array(1, 2, 3, 4, 5)

    sc.makeRDD(data).map(_+"test").collect().foreach(println)
    mapRDD.collect().foreach(println)

  }

}
