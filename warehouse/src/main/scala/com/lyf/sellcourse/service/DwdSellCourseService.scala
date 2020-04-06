package com.lyf.sellcourse.service

import com.alibaba.fastjson.JSONObject
import com.lyf.util.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/06 15:50
  * Version: 1.0
  */
object DwdSellCourseService {
    //导入售课表
    def importSaleCourseLog(sc:SparkContext,ss:SparkSession): Unit ={
       import ss.implicits._
      sc.textFile("/user/lee/ods/salecourse.log").filter(line=>{
        val obj = ParseJsonData.getJsonObject(line)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(rdd=>{
         rdd.map(item=>{
           val jsonObject = ParseJsonData.getJsonObject(item)
           val courseid = jsonObject.getString("courseid")
           val coursename = jsonObject.getString("coursename")
           val status = jsonObject.getString("status")
           val pointlistid = jsonObject.getString("pointlistid")
           val majorid = jsonObject.getString("majorid")
           val chapterid = jsonObject.getString("chapterid")
           val chaptername = jsonObject.getString("chaptername")
           val edusubjectid = jsonObject.getString("edusubjectid")
           val edusubjectname = jsonObject.getString("edusubjectname")
           val teacherid = jsonObject.getString("teacherid")
           val teachername = jsonObject.getString("teachername")
           val coursemanager = jsonObject.getString("coursemanager")
           val money = jsonObject.getString("money")
           val dt = jsonObject.getString("dt")
           val dn = jsonObject.getString("dn")
           (courseid, coursename, status, pointlistid, majorid, chapterid, chaptername,
             edusubjectid, edusubjectname, teacherid, teachername, coursemanager, money, dt, dn)
         })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite)
        .insertInto("dwd.dwd_sale_course")
    }

  /**
    * 导入课程支付信息
    *
    * @param ssc
    * @param sparkSession
    */
  def importCoursePay(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/lee/ods/coursepay.log")
      .filter(item => {
        val obj = ParseJsonData.getJsonObject(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonObject(item)
        val orderid = jsonObject.getString("orderid")
        val paymoney = jsonObject.getString("paymoney")
        val discount = jsonObject.getString("discount")
        val createtime = jsonObject.getString("createitme")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (orderid, discount, paymoney, createtime, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_course_pay")
  }

  /**
    * 导入课程购物车信息
    *
    * @param ssc
    * @param sparkSession
    */
  def importCourseShoppingCart(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/lee/ods/courseshoppingcart.log")
      .filter(item => {
        val obj = ParseJsonData.getJsonObject(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonObject(item)
        val courseid = jsonObject.getString("courseid")
        val orderid = jsonObject.getString("orderid")
        val coursename = jsonObject.getString("coursename")
        val discount = jsonObject.getString("discount")
        val sellmoney = jsonObject.getString("sellmoney")
        val createtime = jsonObject.getString("createtime")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (courseid, orderid, coursename, discount, sellmoney, createtime, dt, dn)
      })
    }).toDF().coalesce(6).write.mode(SaveMode.Append).insertInto("dwd.dwd_course_shopping_cart")
  }


  //导入表并进行分桶
  def importCoursePay2(sc:SparkContext,ss:SparkSession): Unit ={
    import ss.implicits._

    sc.textFile("/user/lee/ods/coursepay.log").filter(line=>{
      val obj = ParseJsonData.getJsonObject(line)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(rdd=>{
      rdd.map(item=>{
        val jsonObject = ParseJsonData.getJsonObject(item)
        val orderid = jsonObject.getString("orderid")
        val paymoney = jsonObject.getString("paymoney")
        val discount = jsonObject.getString("discount")
        val createtime = jsonObject.getString("createitme")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (orderid, discount, paymoney, createtime, dt, dn)
      })
    }).toDF("orderid", "discount", "paymoney", "createtime", "dt", "dn")
//      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .partitionBy("dt","dn")
      .bucketBy(10,"orderid")
      .sortBy("orderid")
      .saveAsTable("dwd.dwd_course_pay_cluster")
  }

  def importCourseShoppingCart2(sc:SparkContext,ss:SparkSession): Unit ={
    import ss.implicits._
    sc.textFile("/user/lee/ods/courseshoppingcart.log").filter(line=>{
      val obj = ParseJsonData.getJsonObject(line)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(rdd=>{
      rdd.map(item=>{
        val jsonObject = ParseJsonData.getJsonObject(item)
        val courseid = jsonObject.getString("courseid")
        val orderid = jsonObject.getString("orderid")
        val coursename = jsonObject.getString("coursename")
        val discount = jsonObject.getString("discount")
        val sellmoney = jsonObject.getString("sellmoney")
        val createtime = jsonObject.getString("createtime")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (courseid, orderid, coursename, discount, sellmoney, createtime, dt, dn)
      })
    }).toDF("courseid", "orderid", "coursename", "discount", "sellmoney", "createtime", "dt", "dn")
//      .coalesce(6)
      .write.mode(SaveMode.Overwrite)
      .partitionBy("dt","dn")
      .bucketBy(10,"orderid")
      .sortBy("orderid")
      .saveAsTable("dwd.dwd_course_shopping_cart_cluster")
  }
}
