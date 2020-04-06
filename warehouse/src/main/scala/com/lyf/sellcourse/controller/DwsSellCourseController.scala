package com.lyf.sellcourse.controller

import com.lyf.sellcourse.service.DwsSellCourseService
import com.lyf.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/06 16:23
  * Version: 1.0
  */
object DwsSellCourseController {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DwsSellCourseController")//.setMaster("local[*]")
      .set("spark.sql.autoBroadcastJoinThreshold","-1") //关闭两表join时，小表自动广播
    val ss: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()


    HiveUtil.openDynamicPartition(ss)
    HiveUtil.openCompress(ss)

    //从dwd层获取数据，join合成售课明细表,存在数据倾斜
    DwsSellCourseService.importSellCourseDetail(ss,"20190722")
    ss.stop()
  }
}
