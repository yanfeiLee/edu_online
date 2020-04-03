package com.lyf.qz.controller

import com.lyf.qz.service.AdsQzService
import com.lyf.util.HiveUtil
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/02 16:29
  * Version: 1.0
  */
object AdsQzController {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AdsQzController")//.setMaster("local[*]")  //提交到集群运行时，不能设置local,否则会覆盖spark2-submit指定的deploy
    val ss: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //开启动态分区
    HiveUtil.openDynamicPartition(ss)
    //ads层数据量较少，不采用压缩
    //提取指标
    val dt = "20190722"
    AdsQzService.getMetricsWithSQL(ss,dt)

    ss.stop()
  }
}
