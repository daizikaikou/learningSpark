package com.zgw.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaogw&Lss on 2019/10/30.
  */
object Accumulators {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Accumulators").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val textFile: RDD[String] = sc.textFile("in/word.txt")
    val blanklines = sc.accumulator(0)
    val mapRDD: RDD[String] = textFile.flatMap(line => {
      if (line == "") {
        blanklines += 1
      }
      line.split(" ")
    })
    mapRDD.saveAsTextFile("out/output.txt")
    println("Blank lines: " + blanklines.value)

  }

}
