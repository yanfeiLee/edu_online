package com.lyf.member.controller

import com.lyf.common.Constant
import com.lyf.member.service.EtlOdsData2DwdService
import com.lyf.util.HiveUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Project: edu_online
  * Create by lyf3312 on 20/03/31 21:00
  * Version: 1.0
  * 将数据从ods层导入到dwd层(hive中)
  */
object DwdMemberController {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DwdMemberController")
    val ss: SparkSession = SparkSession.builder().config(conf)
      .enableHiveSupport()
      .getOrCreate()
    val sc = ss.sparkContext


    //添加配置项目
    HiveUtil.openDynamicPartition(ss)
    HiveUtil.openCompress(ss)


    //调用service层方法，完成数据导入到dws
    EtlOdsData2DwdService.etlBaseAdLog(sc,ss)
    EtlOdsData2DwdService.etlBaseWebSite(sc,ss)
    EtlOdsData2DwdService.etlMember(sc,ss)
    EtlOdsData2DwdService.etlMemberRegType(sc,ss)
    EtlOdsData2DwdService.etlPcenterMemPayMoney(sc,ss)
    EtlOdsData2DwdService.etlPcenterMemVipLevel(sc,ss)

    sc.stop()
    ss.stop()
  }
}
