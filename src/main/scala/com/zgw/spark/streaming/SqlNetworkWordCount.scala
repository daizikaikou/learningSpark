package com.zgw.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * Created by Zhaogw&Lss on 2019/11/22.
  * SparkStream整合SparkSql完整词频统计
  */
object SqlNetworkWordCount {
  def main(args: Array[String]): Unit = {
    val sc: SparkConf = new SparkConf().setMaster("local[3]").setAppName("NetWork").set("spark.testing.memory", "2147480000")

    Logger.getLogger("org").setLevel(Level.ERROR)
    //创建StreamingContext两个参数 SparkConf和batch interval
    val ssc = new StreamingContext(sc, Seconds(5))


    val lines = ssc.socketTextStream("hadoop000", 9999)
    val words = lines.flatMap(_.split(" "))

    // Convert RDDs of the words DStream to DataFrame and run SQL query
    words.foreachRDD { (rdd: RDD[String], time: Time) =>
      // Get the singleton instance of SparkSession
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      // Creates a temporary view using the DataFrame
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    }

    ssc.start()

    ssc.awaitTermination()
  }
  /** Case class for converting RDD to DataFrame */
  case class Record(word: String)


  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }

}
