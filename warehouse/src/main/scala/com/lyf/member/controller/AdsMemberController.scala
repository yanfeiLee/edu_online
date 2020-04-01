package com.lyf.member.controller

import com.lyf.member.service.AdsMemberService
import com.lyf.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/01 20:08
  * Version: 1.0
  */
object AdsMemberController {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AdsMemberController")
    val ss: SparkSession = SparkSession.builder().config(conf)
      .enableHiveSupport()
      .getOrCreate()

    HiveUtil.openDynamicPartition(ss)


    //调用sevice 从dws层表中查询数据
    AdsMemberService.queryDetail(ss,"20190722")
    ss.stop()
  }
}
