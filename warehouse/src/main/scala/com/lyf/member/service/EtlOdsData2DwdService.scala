package com.lyf.member.service

import com.alibaba.fastjson.JSONObject
import com.lyf.common.Constant
import com.lyf.util.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/01 16:02
  * Version: 1.0
  */
object EtlOdsData2DwdService {

  /**
    * @author lyf3312
    * @date 20/04/01 16:21
    * @description baseadlog etl
    */
  def etlBaseAdLog(sc: SparkContext, ss: SparkSession): Unit = {
    import ss.implicits._
    sc.textFile(Constant.ODS_LOG_BASEADLOG)
      .filter(
        line => {
          val obj = ParseJsonData.getJsonObject(line)
          obj.isInstanceOf[JSONObject]
        }
      ).map(
      item => {
        val jsonObject = ParseJsonData.getJsonObject(item)
        val adid = jsonObject.getIntValue("adid")
        val adname = jsonObject.getString("adname")
        val dn = jsonObject.getString("dn")
        (adid, adname, dn)
      })
      .toDF() //利用隐式转换将rdd->df
      .coalesce(1) //缩减分区数
      .write.mode(SaveMode.Overwrite)
      .insertInto(Constant.DWD_TB_BASE_AD)
  }

  /**
    * @author lyf3312
    * @date 20/04/01 16:22
    * @description basewebsite etl
    *
    */
  def etlBaseWebSite(sc: SparkContext, ss: SparkSession): Unit = {
    import ss.implicits._
    sc.textFile(Constant.ODS_LOG_BASEWEBSIT)
      .filter(line => {
        val obj = ParseJsonData.getJsonObject(line)
        obj.isInstanceOf[JSONObject]
      })
      .map(item => {
        val jSONObject = ParseJsonData.getJsonObject(item)
        (jSONObject.getIntValue("siteid"),
          jSONObject.getString("sitename"),
          jSONObject.getString("siteurl"),
          jSONObject.getIntValue("delete"),
          jSONObject.getTimestamp("createtime"),
          jSONObject.getString("creator"),
          jSONObject.getString("dn")
        )
      }).toDF()
      .coalesce(1)
      .write.mode(SaveMode.Overwrite).insertInto(Constant.DWD_TB_BASE_WEBSITE)
  }

  /**
   *  @author lyf3312
   *  @date 20/04/01 16:48
   *  @description etl member
   *
   */
  def etlMember(sc: SparkContext, ss: SparkSession): Unit ={
    import ss.implicits._
    sc.textFile(Constant.ODS_LOG_MEMBER)
      .filter(line=>{
        val obj = ParseJsonData.getJsonObject(line)
        obj.isInstanceOf[JSONObject]
      })
      .map(item=>{
        val jsonObject = ParseJsonData.getJsonObject(item)
        val ad_id = jsonObject.getIntValue("ad_id")
        val birthday = jsonObject.getString("birthday")
        val email = jsonObject.getString("email")
        val fullname = jsonObject.getString("fullname").substring(0, 1) + "xx"
        val iconurl = jsonObject.getString("iconurl")
        val lastlogin = jsonObject.getString("lastlogin")
        val mailaddr = jsonObject.getString("mailaddr")
        val memberlevel = jsonObject.getString("memberlevel")
        val password = "******"
        val paymoney = jsonObject.getString("paymoney")
        val phone = jsonObject.getString("phone")
        val newphone = phone.substring(0, 3) + "*****" + phone.substring(7, 11)
        val qq = jsonObject.getString("qq")
        val register = jsonObject.getString("register")
        val regupdatetime = jsonObject.getString("regupdatetime")
        val uid = jsonObject.getIntValue("uid")
        val unitname = jsonObject.getString("unitname")
        val userip = jsonObject.getString("userip")
        val zipcode = jsonObject.getString("zipcode")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (uid, ad_id, birthday, email, fullname, iconurl, lastlogin, mailaddr, memberlevel, password, paymoney, newphone, qq,
          register, regupdatetime, unitname, userip, zipcode, dt, dn)
      }).toDF()
      .coalesce(2) //如果设置太小，数据量过大，机器内存不足，可能导致oom
      .write.mode(SaveMode.Append).insertInto(Constant.DWD_TB_MEMBER)
  }

  /**
   *  @author lyf3312
   *  @date 20/04/01 16:54
   *  @description etl memberRegType
   *
   */
  def etlMemberRegType(sc: SparkContext, ss: SparkSession): Unit ={
    import ss.implicits._
    sc.textFile(Constant.ODS_LOG_MEMBERREGTYPE)
      .filter(line=>{
        val obj = ParseJsonData.getJsonObject(line)
        obj.isInstanceOf[JSONObject]
      })
      .map(item=>{
        val jsonObject = ParseJsonData.getJsonObject(item)
        val appkey = jsonObject.getString("appkey")
        val appregurl = jsonObject.getString("appregurl")
        val bdp_uuid = jsonObject.getString("bdp_uuid")
        val createtime = jsonObject.getString("createtime")
        val domain = jsonObject.getString("webA")
        val isranreg = jsonObject.getString("isranreg")
        val regsource = jsonObject.getString("regsource")
        val regsourceName = regsource match {
          case "1" => "PC"
          case "2" => "Mobile"
          case "3" => "App"
          case "4" => "WeChat"
          case _ => "other"
        }
        val uid = jsonObject.getIntValue("uid")
        val websiteid = jsonObject.getIntValue("websiteid")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (uid, appkey, appregurl, bdp_uuid, createtime, domain, isranreg, regsource, regsourceName, websiteid, dt, dn)
      }).toDF()
      .coalesce(1)
      .write.mode(SaveMode.Overwrite).insertInto(Constant.DWD_TB_MEMBER_REG_TYPE)
  }

  /**
   *  @author lyf3312
   *  @date 20/04/01 16:59
   *  @description etl percentMemPayMoney
   *
   */
  def etlPcenterMemPayMoney(sc:SparkContext,ss:SparkSession): Unit ={
    import ss.implicits._
    sc.textFile(Constant.ODS_LOG_PCENTERMEMPAYMONEY)
      .filter(line=>{
         ParseJsonData.getJsonObject(line).isInstanceOf[JSONObject]
      })
      .map(item=>{
        val jSONObject = ParseJsonData.getJsonObject(item)
        val paymoney = jSONObject.getString("paymoney")
        val uid = jSONObject.getIntValue("uid")
        val vip_id = jSONObject.getIntValue("vip_id")
        val site_id = jSONObject.getIntValue("siteid")
        val dt = jSONObject.getString("dt")
        val dn = jSONObject.getString("dn")
        (uid, paymoney, site_id, vip_id, dt, dn)
      }).toDF()
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .insertInto(Constant.DWD_TB_PCENTER_MEM_PAYMONEY)
  }

  /**
   *  @author lyf3312
   *  @date 20/04/01 17:09
   *  @description etl memberVipLevel
   *
   */
  def etlPcenterMemVipLevel(sc:SparkContext,ss:SparkSession): Unit ={
    import ss.implicits._
    sc.textFile(Constant.ODS_LOG_PCENTERMEMVIPLEVEL)
      .filter(ParseJsonData.getJsonObject(_).isInstanceOf[JSONObject])
      .mapPartitions(rdd=>{
        rdd.map(item=> {
          val jSONObject = ParseJsonData.getJsonObject(item)
          val discountval = jSONObject.getString("discountval")
          val end_time = jSONObject.getString("end_time")
          val last_modify_time = jSONObject.getString("last_modify_time")
          val max_free = jSONObject.getString("max_free")
          val min_free = jSONObject.getString("min_free")
          val next_level = jSONObject.getString("next_level")
          val operator = jSONObject.getString("operator")
          val start_time = jSONObject.getString("start_time")
          val vip_id = jSONObject.getIntValue("vip_id")
          val vip_level = jSONObject.getString("vip_level")
          val dn = jSONObject.getString("dn")
          (vip_id, vip_level, start_time, end_time, last_modify_time, max_free, min_free, next_level, operator, dn)
        })
      }).toDF().coalesce(2).write.mode(SaveMode.Overwrite).insertInto(Constant.DWD_TB_VIP_LEVEL)
  }
}
