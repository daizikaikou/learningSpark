package com.zgw.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Zhaogw&Lss on 2019/11/26.
  * sparkstreaming对接kafka方式一
  */
object KafkaReceiverWordCount {

  def main(args: Array[String]): Unit = {
    if (args.length!=4){
      System.err.print("Usage:KafkaReceiverWordCount <zkQuorum> <group> <topic> <numThread>")
      System.exit(1)
    }
    /*val Array(hostname,port) = args*/

    val Array(zkQuorum,group,topics,numThread) = args

    val sc: SparkConf = new SparkConf()
    /*.setMaster("local[3]").setAppName("KafkaReceiverWordCount").set("spark.testing.memory", "2147480000")*/

    Logger.getLogger("org").setLevel(Level.ERROR)
    //创建StreamingContext两个参数 SparkConf和batch interval
    val ssc = new StreamingContext(sc, Seconds(5))

    val topicMap = topics.split(",").map((_,numThread.toInt)).toMap

    val messages = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)


    messages.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()

    ssc.awaitTermination()
  }
}
