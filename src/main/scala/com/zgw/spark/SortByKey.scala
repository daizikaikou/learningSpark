package com.zgw.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/16.
  */
object SortByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    /* val rdd = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    //根据key正序排
   rdd.sortByKey(true).collect().foreach(println)
    //倒序排序
    rdd.sortByKey(false).collect().foreach(println)*/
    //mapValue算子针对于(K,V)形式的类型只对V进行操作
    /*val rdd3 = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))
    rdd3.mapValues(_+"|||").collect().foreach(println)*/
    //join算子，在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD
   /* val rdd = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    val rdd1 = sc.parallelize(Array((1,4),(2,5),(3,6)))
    rdd.join(rdd1).collect().foreach(println)*/
    val rdd = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    val rdd1 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    rdd.cogroup(rdd1).collect().foreach(println)


  }

}
