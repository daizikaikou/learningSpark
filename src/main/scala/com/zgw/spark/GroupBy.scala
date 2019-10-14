package com.zgw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/14.
  */
object GroupBy {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("MapPartitions").set("spark.testing.memory", "2147480000")
    //创建spark上下文对象
    val sc = new SparkContext(config)
    //按照制定规则进行分组,分组后的数据形成元组，kv
    val groupRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val groupByRDD: RDD[(Int, Iterable[Int])] = groupRDD.groupBy(_%2)
    groupByRDD.collect().foreach(println)

  }

}
