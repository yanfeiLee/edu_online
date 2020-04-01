package com.lyf.member.dao

import com.lyf.common.Constant
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/01 20:27
  * Version: 1.0
  */
object AdsMemberDao {

  //查询数据，写入到ads层的表中
  //统计通过各注册跳转地址(appregurl)进行注册的用户数
  def queryAppregurlCount(ss: SparkSession, dt: String): Unit = {
    ss.sql(s"select appregurl,count(uid) num,dt,dn from ${Constant.DWS_TB_MEMBER} where dt='${dt}' group by appregurl,dn,dt")
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto(Constant.ADS_TB_REGISTER_APPREGURLNUM)
  }

  //统计各所属网站（sitename）的用户数
  def querySiteNameCount(ss: SparkSession, dt: String): Unit = {
    ss.sql(s"select sitename,count(uid),dn,dt from dws.dws_member where dt='${dt}' group by sitename,dn,dt")
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto(Constant.ADS_TB_REGISTER_SITENAMENUM)
  }

  //统计各所属平台的（regsourcename）用户数
  def queryRegsourceNameCount(ss: SparkSession, dt: String): Unit = {
    ss.sql(s"select regsourcename,count(uid),dn,dt from dws.dws_member where dt='${dt}' group by regsourcename,dn,dt")
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto(Constant.ADS_TB_REGISTER_REGSOURCENAMENUM)
  }

  //统计通过各广告跳转（adname）的用户数
  def queryAdNameCount(ss: SparkSession, dt: String): Unit = {
    ss.sql(s"select adname,count(uid),dn,dt from dws.dws_member where dt='${dt}' group by adname,dn,dt")
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto(Constant.ADS_TB_REGISTER_ADNAMENUM)
  }

  //统计各用户级别（memberlevel）的用户数
  def queryMemberLevelCount(ss: SparkSession, dt: String): Unit = {
    ss.sql(s"select memberlevel,count(uid),dn,dt from dws.dws_member where dt='${dt}' group by memberlevel,dn,dt")
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto(Constant.ADS_TB_REGISTER_MEMBERLEVELNUM)
  }

  //统计各vip等级人数
  def queryVipLevelCount(ss: SparkSession, dt: String): Unit = {
    ss.sql(s"select vip_level,count(uid),dn,dt from dws.dws_member group where dt='${dt}' group by vip_level,dn,dt")
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto(Constant.ADS_TB_REGISTER_VIPLEVELNUM)
  }

  //统计各分区网站、用户级别下(website、memberlevel)的支付金额top3用户
  def getTop3MemberLevelPayMoneyUser(ss: SparkSession, dt: String): Unit = {
    ss.sql(
      s"""
         |select
         |uid,
         |memberlevel,
         |register,
         |appregurl,
         |regsourcename,
         |adname,
         |sitename,
         |vip_level,
         |py4 paymoney,
         |rownum,
         |dt,
         |dn
         |from (
         |    select
         |           uid,
         |           ad_id,
         |           memberlevel,
         |           register,
         |           appregurl,
         |           regsource,
         |           regsourcename,
         |           adname,
         |           siteid,
         |           sitename,
         |           vip_level,
         |           cast(paymoney as decimal(10,4)) py4,
         |           row_number() over(partition by dn,memberlevel order by cast(paymoney as decimal(10,4)) desc ) as rownum,
         |           dn,
         |           dt
         |    from dws.dws_member
         |    where dt=${dt}
         |)
         |where rownum < 4
         |order by memberlevel,rownum
       """.stripMargin)
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto(Constant.ADS_TB_REGISTER_TOP3MEMBERPAY)
  }
}
