package com.zgw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/11.
  */
object Spark_RDD {
  def main(args: Array[String]): Unit = {

    val config = new SparkConf().setMaster("local[*]").setAppName("Spark_RDD").set("spark.testing.memory","2147480000")
    //创建spark上下文对象
    val sc = new SparkContext(config)
    //创建rdd，从内存中创建makeRDD,底层就是parallelize
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,5,4))
    listRDD.collect().foreach(println)
    //从内存中创建parallelize
    val arrayRDD: RDD[Int] = sc.parallelize(Array(1,2,3,4))

    arrayRDD.collect().foreach(println)


    //从外部存储创建,默认项目路径，也可以改为hdfs路径hdfs://hadoop102:9000/xxx
    val fileRDD: RDD[String] = sc.textFile("in")
    fileRDD.foreach(println)
  }

}
