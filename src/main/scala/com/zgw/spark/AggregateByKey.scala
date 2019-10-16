package com.zgw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/16.
  */
object AggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    //aggregateByKeyRDD算子
    val rdd = sc.parallelize(List(("a",3),("c",4),("b",3),("c",6),("c",8),("a",2)),2)
    val aggregateByKeyRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_,_),_+_)
    aggregateByKeyRDD.collect().foreach(println)
  }

}
