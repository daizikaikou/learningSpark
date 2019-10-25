package com.zgw.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Zhaogw&Lss on 2019/10/25.
  */
object UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    var sparkConf =new SparkConf().setMaster("local[*]").setAppName("UpdateStateByKey").set("spark.testing.memory", "2147480000")
   //分析环境对象以及采集周期
    val streamContext = new StreamingContext(sparkConf,Seconds(10))
    streamContext.checkpoint("hdfs://192.168.181.110:9000/spark-streaming")
    val socketStreamLine: ReceiverInputDStream[String] = streamContext.socketTextStream("192.168.181.110",9999)

    val print1: Unit = socketStreamLine.flatMap(_.split(" ")).map((_, 1))
      .updateStateByKey[Int](updateFunction _).print()

    //启动采集器
    streamContext.start()
    //等待采集器执行
    streamContext.awaitTermination()

  }

  /**
    *
    * @param currentValues
    * @param preValues
    * @return
    */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current + pre)
  }

}
