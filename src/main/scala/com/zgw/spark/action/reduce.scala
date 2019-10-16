package com.zgw.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/16.
  */
object reduce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    //reduce算子
   /* val makeRDD: RDD[Int] = sc.makeRDD(1 to 20, 3)
    val reduce1: Int = makeRDD.reduce(_ + _)
    println(reduce1)
    val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 2), ("c", 3), ("d", 4)))
    val reduce2: (String, Int) = rdd.reduce((x, y) => (x._1, x._2 + y._2))
    println(reduce2)*/
    //collect算子
  /* val rdd = sc.parallelize(1 to 10)
    println(rdd.collect.mkString(","))*/
  //count算子
   /* val rdd = sc.parallelize(1 to 10)
    println(rdd.count)*/
    //first算子
    /*val rdd = sc.parallelize(1 to 10)
    println(rdd.first)*/
    //take算子
   /* val rdd = sc.parallelize(Array(2,5,4,6,8,3))
    println(rdd.take(3).mkString(","))*/
  // aggregate算子
    /*var rdd1 = sc.makeRDD(1 to 10,2)
    println(rdd1.aggregate(0)(_ + _, _ + _))*/
    val rdd: RDD[Int] = sc.makeRDD(1 to 10,2)
    println(rdd.aggregate(0)(_ + _, _ + _))
    //countByKey算子
   /* val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    println(rdd.countByKey)*/
   //foreach算子
   /* var rdd = sc.makeRDD(1 to 5,2)
    rdd.foreach(println(_))*/
  }
}
