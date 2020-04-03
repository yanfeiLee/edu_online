package com.lyf.qz.controller

import com.lyf.qz.service.DwdQzService
import com.lyf.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/02 15:03
  * Version: 1.0
  */
object DwsQzController {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DwsQzController")//.setMaster("local[*]")
    val ss: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()


    HiveUtil.openDynamicPartition(ss)
    HiveUtil.openCompress(ss)

    //轻度聚合
    val dt = "20190722"
    //维度退化
    DwdQzService.saveDwsQzChapter(ss,dt)
    DwdQzService.saveDwsQzCourse(ss,dt)
    DwdQzService.saveDwsQzMajor(ss,dt)
    DwdQzService.saveDwsQzPaper(ss,dt)
    DwdQzService.saveDwsQzQuestionTpe(ss,dt)
    //用dwd层的用户做题详情表和dws层5张降维表合成用户做题详情大宽表
    DwdQzService.saveDwsUserPaperDetail(ss,dt)
    ss.stop()
  }
}
