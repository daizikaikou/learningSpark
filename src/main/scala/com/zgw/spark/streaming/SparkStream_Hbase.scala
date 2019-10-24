package com.zgw.spark.streaming


import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import scala.collection.Iterator
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes


/**
  * Created by Zhaogw&Lss on 2019/10/23.
  */
object SparkStream_Hbase {
  def main(args: Array[String]): Unit = {
    //Spark配置项
    val conf = new SparkConf()
      .setAppName("SocketWordFreq")
      .setMaster("local[*]").set("spark.testing.memory", "2147480000")
    //创建流式上下文，2s的批处理间隔
    val ssc = new StreamingContext(conf, Seconds(8))
    //创建一个DStream，连接指定的hostname:port，比如master:9999
    val lines = ssc.socketTextStream("dblab-VirtualBox", 9999) //DS1
    //将接收到的每条信息分割成单个词汇
    val words = lines.flatMap(_.split(" ")) //DS2
    //统计每个batch的词频
    val pairs = words.map(word => (word, 1)) //DS3
    // 汇总词汇
    val wordCounts = pairs.reduceByKey(_ + _) //DS4

    wordCounts.print()
    //在reduce聚合之后，输出结果至HBase（输出操作）
    wordCounts.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
      //RDD为空时，无需再向下执行，否则在分区中还需要获取数据库连接（无用操作）
      if (!rdd.isEmpty()) {
        //一个分区执行一批SQL
        rdd.foreachPartition((partition: Iterator[(String, Int)]) => {
          //每个分区都会创建一个task任务线程，分区多，资源利用率高
          //可通过参数配置分区数："--conf spark.default.parallelism=20"
          if (!partition.isEmpty) {
            //partition和record共同位于本地计算节点Worker，故无需序列化发送conn和statement
            //如果多个分区位于一个Worker中，则共享连接（位于同一内存资源中）
            //获取HBase连接
            val conn = HbaseUtil.getHBaseConn
            if (conn == null) {
              println("conn is null.") //在Worker节点的Executor中打印
            } else {
              println("conn is not null." + Thread.currentThread().getName())
              partition.foreach((record: (String, Int)) => {
                //每个分区中的记录在同一线程中处理
                println("record : " + Thread.currentThread().getName())
                //设置表名
                val tableName = TableName.valueOf("wordfreq")
                //获取表的连接
                val table = conn.getTable(tableName)
                try {
                  //设定行键（单词）
                  val put = new Put(Bytes.toBytes(record._1))
                  //添加列值（单词个数）
                  //三个参数：列族、列、列值
                  put.addColumn(Bytes.toBytes("statistics"),
                    Bytes.toBytes("cnt"),
                    Bytes.toBytes(record._2))
                  //执行插入
                  table.put(put)
                  println("insert (" + record._1 + "," + record._2 + ") into hbase success.")
                } catch {
                  case e: Exception => e.printStackTrace()
                } finally {
                  table.close()
                }
              })
              //关闭HBase连接（此处每个partition任务结束都会执行，会频繁开关连接，耗费资源）
              //              HbaseUtil.closeHbaseConn()
            }
          }
        })
        //关闭HBase连接（此处只在Driver节点执行，故无效）
        //        HbaseUtil.closeHbaseConn()
      }
    })
    //打印从DStream中生成的RDD的前10个元素到控制台中
    wordCounts.print() //print() 是输出操作，默认前10条数据
    ssc.start() //开始计算
    ssc.awaitTermination() //等待计算结束
  }
}
