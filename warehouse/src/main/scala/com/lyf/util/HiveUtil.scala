package com.lyf.util

import org.apache.spark.sql.SparkSession

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/01 15:24
  * Version: 1.0
  */
object HiveUtil {

  /**
    * @author lyf3312
    * @date 20/04/01 15:38
    * @description 设置hive最大分区数
    *
    */
  def setMaxPartition(ss: SparkSession): Unit = {
    //在所有执行MR的节点上，最大一共可以创建多少个动态分区，默认值1000
    ss.sql("set hive.exec.max.dynamic.partitions=100000")
    //在每个执行MR的节点上，最大可以创建多少个动态分区，默认值100
    ss.sql("set hive.exec.max.dynamic.partitions.pernode=100000")
    //整个MR Job中，最大可以创建多少个HDFS文件，默认值100000
    ss.sql("set hive.exec.max.created.files=100000")
  }


  /**
    * @author lyf3312
    * @date 20/04/01 15:42
    * @description 开启动态分区
    *
    */
  def openDynamicPartition(ss: SparkSession): Unit = {
    //开启动态分区
    ss.sql("set hive.exec.dynamic.partition=true")
    //设置动态分区的模式为非严格模式
    ss.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  }

  /**
    * @author lyf3312
    * @date 20/04/01 15:48
    * @description 开启最终输出压缩功能
    *
    */
  def openCompress(ss: SparkSession): Unit = {
    ss.sql("set hive.exec.compress.output=true")
    ss.sql("set mapred.output.compress=true")
  }

  /**
   *  @author lyf3312
   *  @date 20/04/01 15:53
   *  @description 采用snappy压缩
   *
   */
  def useSnappyCompress(ss: SparkSession): Unit = {
    ss.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    ss.sql("set mapreduce.output.fileoutputformat.compress=true")
    ss.sql("set mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.SnappyCodec")
  }

  /**
   *  @author lyf3312
   *  @date 20/04/01 15:55
   *  @description Lzo压缩
   *
   */
  def useLzoCompress(ss:SparkSession): Unit ={
    ss.sql("set io.compression.codec.lzo.class=com.hadoop.compression.lzo.LzoCodec")
    ss.sql("set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec")
  }
}
