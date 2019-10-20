package com.zgw.spark.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

/**
  * Created by Zhaogw&Lss on 2019/10/20.
  */
object SparkSqlUDAF {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    //sparkSession,不能直接new，私有的对象,具体看源码
    val spark = SparkSession.builder.config(conf).getOrCreate()
    //这里是sparksession的名字
    import spark.implicits._
    //自定义聚合函数
    //创建聚合函数对象
    val uadf = new MyAvgFunt
    //注册聚合函数
    spark.udf.register("avgage",uadf)
    val dataFrame: DataFrame = spark.read.json("in/user.json")
    dataFrame.createOrReplaceTempView("user")
    spark.sql("select avgage(age) from user").show()

  }

  }
//自定义聚合函数
//1)继承UserDefinedAggregateFunction
class MyAvgFunt extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = {
    new StructType().add("age",LongType)
  }
//计算时数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }
  //返回数据类型
  override def dataType: DataType = DoubleType
//函数是否稳定
  override def deterministic: Boolean =true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L
    buffer(1)=0L

  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getLong(0)+input.getLong(0)
    buffer(1)=buffer.getLong(1)+1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)

  }

  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
