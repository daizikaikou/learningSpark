package com.zgw.spark.sparkSQL

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/18.
  */
object sparksql_demo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    //sparkSession,不能直接new，私有的对象,具体看源码
    val session = SparkSession.builder.config(conf).getOrCreate()
    val dataFrame: DataFrame = session.read.json("in/user.json")
    //展示数据
    /*dataFrame.show()*/
    /*将dataframe转换为表*/
    val view: Unit = dataFrame.createTempView("table1")
    session.sql("select * from table1").show()


    session.stop()

  }

}
