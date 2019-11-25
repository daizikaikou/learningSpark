package com.zgw.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Zhaogw&Lss on 2019/11/25.
  * SparkStreaming整合Flume的第一种方式
  */
object FlumePushWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length!=2){
      System.err.print("Usage:FlumePushWordCount <hostname> <port>")
      System.exit(1)
    }
    val Array(hostname,port) = args

    val sc: SparkConf = new SparkConf()
      /*.setMaster("local[3]").setAppName("FlumePushWordCount").set("spark.testing.memory", "2147480000")
*/
    Logger.getLogger("org").setLevel(Level.ERROR)
    //创建StreamingContext两个参数 SparkConf和batch interval
    val ssc = new StreamingContext(sc, Seconds(5))

    val flumeStream = FlumeUtils.createStream(ssc,hostname,port.toInt)

    flumeStream.map(x => new String(x.event.getBody.array()).trim).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()


    ssc.start()

    ssc.awaitTermination()
  }

}
