package com.zgw.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/15.
  */
object Value {
  def main(args: Array[String]): Unit = {

    val config = new SparkConf().setMaster("local[*]").setAppName("Spark_RDD").set("spark.testing.memory", "2147480000")
    //创建spark上下文对象
    val sc = new SparkContext(config)
    //union操作，并集
   /* val rdd1 = sc.parallelize(1 to 5)
    val rdd2 = sc.parallelize(5 to 10)
    val rdd3 = rdd1.union(rdd2)
    rdd3.collect().foreach(println)*/
    //求差集
   /* val rdd4 = sc.parallelize(3 to 8)
    val rdd5 = sc.parallelize(1 to 5)
    var subrdd = rdd4.subtract(rdd5)
    subrdd.collect().foreach(println)*/
   //计算两个RDD的交集
   /* val rdd6 = sc.parallelize(1 to 7)
    val rdd7 = sc.parallelize(5 to 10)
    val rdd8 = rdd6.intersection(rdd7)
    rdd8.collect().foreach(println)*/
    //计算两个RDD的笛卡尔积并打印
   /* val rdd9 = sc.parallelize(1 to 3)
    val rdd10 = sc.parallelize(2 to 5)
    val rdd11 = rdd9.cartesian(rdd10)
    rdd11.collect().foreach(println)*/
    //将两个RDD组合成Key/Value形式的RDD,这里默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常。
    val rdd12 = sc.parallelize(Array(1,2,3),3)
    val rdd13 = sc.parallelize(Array("a","b","c"),3)
    val collectrdd12: Array[(Int, String)] = rdd12.zip(rdd13).collect()
    val collectrdd13: Array[(String, Int)] = rdd13.zip(rdd12).collect()
    collectrdd12.foreach(println)
    println("**********")
    collectrdd13.foreach(println)

  }

}
