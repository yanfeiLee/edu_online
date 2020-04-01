package com.lyf.member.controller

import com.lyf.member.service.DwsMemberService
import com.lyf.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/01 17:14
  * Version: 1.0
  */
object DwsMemberController {
  def main(args: Array[String]): Unit = {
    //设置hadooop用户
    System.setProperty("HADOOP_USER_NAME", "lee")

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DwsMemberController")
    val ss: SparkSession = SparkSession.builder().config(conf)
      .enableHiveSupport()
      .getOrCreate()
    val sc = ss.sparkContext

    //设置读取的文件系统，也可以从配置文件中读取
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    //配置开启动态分区
    HiveUtil.openDynamicPartition(ss)
    //配置压缩
    HiveUtil.openCompress(ss)
//    HiveUtil.useSnappyCompress(ss)

    //从dwd 轻度聚合得到dws宽表
    //根据用户信息聚合用户表数据
    DwsMemberService.importMember(ss,"20190722")
    ss.stop()
  }
}
