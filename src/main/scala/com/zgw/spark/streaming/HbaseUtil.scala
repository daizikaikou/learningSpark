package com.zgw.spark.streaming


import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}

/**
  * Created by Zhaogw&Lss on 2019/10/23.
  */

object HbaseUtil extends Serializable {
  //配置信息
  private val conf = HBaseConfiguration.create()
  conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
  conf.set(HConstants.ZOOKEEPER_QUORUM, "dblab-VirtualBox")
  //HBase连接
  @volatile private var connection: Connection = _
  //请求的连接数计数器（为0时关闭）
  @volatile private var num = 0

  //获取HBase连接
  def getHBaseConn: Connection = {
    synchronized {
      if (connection == null || connection.isClosed() || num == 0) {
        connection = ConnectionFactory.createConnection(conf)
        println("conn is created! " + Thread.currentThread().getName())
      }
      //每请求一次连接，计数器加一
      num = num + 1
      println("request conn num: " + num + " " + Thread.currentThread().getName())
    }
    connection
  }

  //关闭HBase连接
  def closeHbaseConn(): Unit = {
    synchronized {
      if (num <= 0) {
        println("no conn to close!")
        return
      }
      //每请求一次关闭连接，计数器减一
      num = num - 1
      println("request close num: " + num + " " + Thread.currentThread().getName())
      //请求连接计数器为0时关闭连接
      if (num == 0 && connection != null && !connection.isClosed()) {
        connection.close()
        println("conn is closed! " + Thread.currentThread().getName())
      }
    }
  }

}
