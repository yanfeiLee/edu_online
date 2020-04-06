package com.lyf.sellcourse.controller

import com.lyf.sellcourse.service.DwdSellCourseService
import com.lyf.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/06 15:46
  * Version: 1.0
  * 导入数据并创建分桶表
  */
object DwdSellCourseController2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DwdSellCourseController2")//.setMaster("local[*]")
    val ss: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc = ss.sparkContext

    HiveUtil.openDynamicPartition(ss)
    HiveUtil.openCompress(ss)

    //调用sevice层实现数据从ods层进行etl到dwd层
    DwdSellCourseService.importSaleCourseLog(sc,ss)
    DwdSellCourseService.importCoursePay2(sc,ss)
    DwdSellCourseService.importCourseShoppingCart2(sc,ss)

    ss.stop()
  }
}
