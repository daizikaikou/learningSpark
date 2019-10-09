package com.zgw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/9.
  */
object WordCount{
  def main(args: Array[String]) = {
    //使用开发工具完成wordcount开发
    //创建sparkconf对象设定运行环境
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory","2147480000")
    //创建spark上下文对象
    val sc = new SparkContext(config)

    //将文件内容读取
    val lines: RDD[String] = sc.textFile("in/word.txt")
    //将一行一行的数据转换为单词
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //将单词结构进行转换
    val wordToOne: RDD[(String, Int)] = words.map((_,1))

    //对转换后的结果进行分组聚合
    val reduceByKey: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)

    //将统计结果打印到控制台
    val result: Array[(String, Int)] = reduceByKey.collect()

    result.foreach(println)
  }

}
