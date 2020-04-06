package com.lyf.sellcourse.dao

import org.apache.spark.sql.SparkSession

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/06 16:36
  * Version: 1.0
  */
object DwdSellCourseDao {
  def getDwdSaleCourse(ss: SparkSession) = {
    ss.sql("select courseid,coursename,status,pointlistid,majorid,chapterid,chaptername,edusubjectid," +
      "edusubjectname,teacherid,teachername,coursemanager,money,dt,dn from dwd.dwd_sale_course")
  }

  def getDwdCourseShoppingCart(sparkSession: SparkSession) = {
    sparkSession.sql("select courseid,orderid,coursename,discount,sellmoney,createtime,dt,dn from dwd.dwd_course_shopping_cart")
  }

  def getDwdCoursePay(sparkSession: SparkSession) = {
    sparkSession.sql("select orderid,discount,paymoney,createtime,dt,dn from dwd.dwd_course_pay")
  }

  //查询分桶表数据
  def getDwdCourseShoppingCart2(sparkSession: SparkSession) = {
    sparkSession.sql("select courseid,orderid,coursename,discount,sellmoney,createtime,dt,dn from dwd.dwd_course_shopping_cart_cluster")
  }

  def getDwdCoursePay2(sparkSession: SparkSession) = {
    sparkSession.sql("select orderid,discount,paymoney,createtime,dt,dn from dwd.dwd_course_pay_cluster")
  }
}
