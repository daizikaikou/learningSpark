package com.zgw.spark.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by Zhaogw&Lss on 2019/10/18.
  */
object spark_sql_transform1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    //sparkSession,不能直接new，私有的对象,具体看源码
    val spark = SparkSession.builder.config(conf).getOrCreate()
    //这里是sparksession的名字
    import spark.implicits._

    //创建rdd，并转为df，再转化为ds，再转化为df，再转为rdd
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",10)))
    //转为df
    val userrdd: RDD[User] = rdd.map{
    case (id, name, age) => {
        User(id, name, age)
      }}

    val dataset: Dataset[User] = userrdd.toDS()
    val rdd1: RDD[User] = dataset.rdd
    rdd1.foreach(println)

  }


}
