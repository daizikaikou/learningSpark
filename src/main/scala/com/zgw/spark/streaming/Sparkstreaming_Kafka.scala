package com.zgw.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by Zhaogw&Lss on 2019/10/22.
  */
object Sparkstreaming_Kafka {
  def main(args: Array[String]): Unit = {

        var sparkConf =new SparkConf().setMaster("local[*]").setAppName("SparkStream").set("spark.testing.memory", "2147480000")

        //分析环境对象以及采集周期
        val streamContext = new StreamingContext(sparkConf,Seconds(25))

      /*  val inputFile = "hdfs://192.168.181.128:8020/spark/"*/

       //kafka Stream
    val kafkaDsream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamContext,
      "dblab-VirtualBox:2181",
      "zhaogw",
      Map("zhaogw" -> 3)
    )

        //将采集数据进行分解
        val dStream: DStream[String] = kafkaDsream.flatMap(t=> t._2.split(" "))

        //将数据进行结构转变
        val map: DStream[(String, Int)] = dStream.map((_,1))
        //聚合处理
        val key: DStream[(String, Int)] = map.reduceByKey(_+_)
        //结果打印
        key.print()
        //启动采集器
        streamContext.start()
        //等待采集器执行
        streamContext.awaitTermination()

  }

}
