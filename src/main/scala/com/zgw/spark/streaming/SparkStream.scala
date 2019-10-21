package com.zgw.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Zhaogw&Lss on 2019/10/21.
  */
object SparkStream {
  def main(args: Array[String]): Unit = {

    var sparkConf =new SparkConf().setMaster("local[*]").setAppName("SparkStream").set("spark.testing.memory", "2147480000")

    //分析环境对象以及采集周期
    val streamContext = new StreamingContext(sparkConf,Seconds(3))

    //从指定端口采集数据
    val socketStreamLine: ReceiverInputDStream[String] = streamContext.socketTextStream("localhost",9999)

    //将采集数据进行分解
    val dStream: DStream[String] = socketStreamLine.flatMap(line => line.split(" "))

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
