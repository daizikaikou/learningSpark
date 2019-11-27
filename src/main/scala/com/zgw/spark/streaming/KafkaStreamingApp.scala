package com.zgw.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by Zhaogw&Lss on 2019/11/27.
  */
object KafkaStreamingApp {
  def main(args: Array[String]): Unit = {
    if (args.length!=2){
      System.err.print("Usage:KafkaReceiverWordCount <brokers> <topic> ")
      System.exit(1)
    }
    /*val Array(hostname,port) = args*/

    val Array(brokers,topics) = args

    val sc: SparkConf = new SparkConf().setMaster("local[3]").setAppName("KafkaReceiverWordCount").set("spark.testing.memory", "2147480000")

    Logger.getLogger("org").setLevel(Level.ERROR)
    //创建StreamingContext两个参数 SparkConf和batch interval
    val ssc = new StreamingContext(sc, Seconds(5))

    val topicSet = topics.split(",").toSet

    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers)

    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicSet)


    messages.map(_._2).count().print()

    ssc.start()

    ssc.awaitTermination()
  }
}
