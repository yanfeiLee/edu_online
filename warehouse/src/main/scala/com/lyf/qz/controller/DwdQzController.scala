package com.lyf.qz.controller

import com.lyf.qz.service.EtlOdsData2DwdService
import com.lyf.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/02 14:07
  * Version: 1.0
  */
object DwdQzController {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DwdQzController")//.setMaster("local[*]")
    val ss: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc = ss.sparkContext

    //开启动态分区及压缩
    HiveUtil.openDynamicPartition(ss)
    HiveUtil.openCompress(ss) //CDH默认采用snappy 压缩方式

    //从ods导数据到dwd层，并进行etl

    EtlOdsData2DwdService.etlQzChapter(sc, ss)
    EtlOdsData2DwdService.etlQzChapterList(sc, ss)
    EtlOdsData2DwdService.etlQzPoint(sc, ss)
    EtlOdsData2DwdService.etlQzPointQuestion(sc, ss)
    EtlOdsData2DwdService.etlQzSiteCourse(sc, ss)
    EtlOdsData2DwdService.etlQzCourse(sc, ss)
    EtlOdsData2DwdService.etlQzCourseEdusubject(sc, ss)
    EtlOdsData2DwdService.etlQzWebsite(sc, ss)
    EtlOdsData2DwdService.etlQzMajor(sc, ss)
    EtlOdsData2DwdService.etlQzBusiness(sc, ss)
    EtlOdsData2DwdService.etlQzPaperView(sc, ss)
    EtlOdsData2DwdService.etlQzCenterPaper(sc, ss)
    EtlOdsData2DwdService.etlQzPaper(sc, ss)
    EtlOdsData2DwdService.etlQzCenter(sc, ss)
    EtlOdsData2DwdService.etlQzQuestion(sc, ss)
    EtlOdsData2DwdService.etlQzQuestionType(sc, ss)
    EtlOdsData2DwdService.etlQzMemberPaperQuestion(sc, ss)

    ss.stop()
  }
}
