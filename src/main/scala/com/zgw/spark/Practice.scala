package com.zgw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/16.
  */
object Practice {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val file: RDD[String] = sc.textFile("agent.log")
    //统计每个省份广告被点击次数top3
    /*val mapRDD: RDD[(String, String)] = file.map(line=>(line.split(" ")(1),line.split(" ")(4)))
    mapRDD.collect().foreach(println)*/
    //3.按照最小粒度聚合：((Province,AD),1)
    val provinceAdToOne = file.map { x =>
      val fields: Array[String] = x.split(" ")
      ((fields(1), fields(4)), 1)
    }
    //4.计算每个省中每个广告被点击的总数：((Province,AD),sum)
   val reduceByKeyRDD: RDD[((String, String), Int)] = provinceAdToOne.reduceByKey(_+_)
    //5.将省份作为key，广告加点击数为value：(Province,(AD,sum))
    val map: RDD[(String, (String, Int))] = reduceByKeyRDD.map(x => (x._1._1,(x._1._2,x._2)))
    //6.将同一个省份的所有广告进行聚合(Province,List((AD1,sum1),(AD2,sum2)...))
    val groupByKey: RDD[(String, Iterable[(String, Int)])] = map.groupByKey()
    //7.对同一个省份所有广告的集合进行排序并取前3条，排序规则为广告点击总数
    val provinceAdTop3 = groupByKey.mapValues { x =>
      x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    }
    provinceAdTop3.collect().foreach(println)
    sc.stop()
  }

}
