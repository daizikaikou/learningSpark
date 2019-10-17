package com.zgw.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/17.
  */
object SeriaTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))
    val search = new Search("h")
    val match1: RDD[String] = search.getMatche1(rdd)
    match1.collect().foreach(println)

  }

}
class Search(string: String) extends java.io.Serializable{

  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(string)
  }

  //过滤出包含字符串的RDD
  def getMatche1 (rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatche2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains(rdd))
  }

}
