package com.lyf.sellcourse.service

import java.sql.Timestamp

import com.lyf.sellcourse.beans.{DwdCourseShoppingCart, DwdSaleCourse}
import com.lyf.sellcourse.dao.DwdSellCourseDao
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/06 16:26
  * Version: 1.0
  */
object DwsSellCourseService {

  //三张表正常join=》会出现数据倾斜
  def importSellCourseDetail(ss: SparkSession, dt: String): Unit = {
    //1w条数据
    val dwdSellCourse = DwdSellCourseDao.getDwdSaleCourse(ss).where(s"dt=${dt}")
    //3000w条数据
    val dwdCourseShoppingCart = DwdSellCourseDao.getDwdCourseShoppingCart(ss).where(s"dt='${dt}'")
      .drop("coursename") //删除重复列
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    //2000w条数据
    val dwdCoursePay = DwdSellCourseDao.getDwdCoursePay(ss).where(s"dt='${dt}'")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    dwdSellCourse.join(dwdCourseShoppingCart, Seq("courseid", "dt", "dn"), "right")
      .join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }

  //解决数据倾斜方案一：小表定倍扩容(100倍)，大表随机打散
  def importSellCourseDetail2(ss: SparkSession, dt: String): Unit = {
    //1w条数据
    val dwdSellCourse = DwdSellCourseDao.getDwdSaleCourse(ss).where(s"dt=${dt}")
    //3000w条数据
    val dwdCourseShoppingCart = DwdSellCourseDao.getDwdCourseShoppingCart(ss).where(s"dt='${dt}'")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    //2000w条数据
    val dwdCoursePay = DwdSellCourseDao.getDwdCoursePay(ss).where(s"dt='${dt}'")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    import ss.implicits._
    //小表定倍扩容
    val newSaleCourse = dwdSellCourse.flatMap(item => {
      val list = new ArrayBuffer[DwdSaleCourse]()
      val courseid = item.getAs[Int]("courseid")
      val coursename = item.getAs[String]("coursename")
      val status = item.getAs[String]("status")
      val pointlistid = item.getAs[Int]("pointlistid")
      val majorid = item.getAs[Int]("majorid")
      val chapterid = item.getAs[Int]("chapterid")
      val chaptername = item.getAs[String]("chaptername")
      val edusubjectid = item.getAs[Int]("edusubjectid")
      val edusubjectname = item.getAs[String]("edusubjectname")
      val teacherid = item.getAs[Int]("teacherid")
      val teachername = item.getAs[String]("teachername")
      val coursemanager = item.getAs[String]("coursemanager")
      val money = item.getAs[java.math.BigDecimal]("money")
      val dt = item.getAs[String]("dt")
      val dn = item.getAs[String]("dn")
      for (i <- 0 until 100) {
        list.append(DwdSaleCourse(courseid, coursename, status, pointlistid, majorid, chapterid, chaptername, edusubjectid,
          edusubjectname, teacherid, teachername, coursemanager, money, dt, dn, courseid + "_" + i))
      }
      list.iterator
    })
    //大表的key打散，拼接随机后缀
    val newCourseShoppingCart = dwdCourseShoppingCart.mapPartitions(rdd => {
      rdd.map(item => {
        val courseid = item.getAs[Int]("courseid")
        val randInt = Random.nextInt(100)
        DwdCourseShoppingCart(courseid, item.getAs[String]("orderid"),
          item.getAs[String]("coursename"), item.getAs[java.math.BigDecimal]("cart_discount"),
          item.getAs[java.math.BigDecimal]("sellmoney"), item.getAs[Timestamp]("cart_createtime"),
          item.getAs[String]("dt"), item.getAs[String]("dn"), courseid + "_" + randInt)
      })
    })
    //利用新生成的DS saleCourse和CourseShoppingCart 按照rand_courseid进行join
    newSaleCourse.join(newCourseShoppingCart.drop("courseid").drop("coursename"),
      Seq("rand_courseid", "dt", "dn"), "right")
      .join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")

  }

  //解决数据倾斜方案二：小表广播
  def importSellCourseDetail3(ss: SparkSession, dt: String) = {
    //1w条数据
    val dwdSellCourse = DwdSellCourseDao.getDwdSaleCourse(ss).where(s"dt=${dt}")
    //3000w条数据
    val dwdCourseShoppingCart = DwdSellCourseDao.getDwdCourseShoppingCart(ss).where(s"dt='${dt}'")
      .drop("coursename") //删除重复列
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    //2000w条数据
    val dwdCoursePay = DwdSellCourseDao.getDwdCoursePay(ss).where(s"dt='${dt}'")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")

    //导入扩展函数
    import org.apache.spark.sql.functions._
    broadcast(dwdSellCourse).join(dwdCourseShoppingCart, Seq("courseid", "dt", "dn"), "right")
      .join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }

  //解决数据倾斜方案三：采用广播小表  两个大表进行分桶并且进行SMB join，join以后的大表再与广播后的小表进行join
  def importSellCourseDetail4(ss: SparkSession, dt: String) = {
    //1w条数据
    val dwdSellCourse = DwdSellCourseDao.getDwdSaleCourse(ss).where(s"dt=${dt}")
    //3000w条数据
    val dwdCourseShoppingCart = DwdSellCourseDao.getDwdCourseShoppingCart2(ss).where(s"dt='${dt}'")
      .drop("coursename") //删除重复列
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    //2000w条数据
    val dwdCoursePay = DwdSellCourseDao.getDwdCoursePay2(ss).where(s"dt='${dt}'")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    //导入扩展函数
    import org.apache.spark.sql.functions._
    val temp = dwdCourseShoppingCart.join(dwdCoursePay, Seq("orderid"), "left")
    val bd = broadcast(dwdSellCourse)
    bd.join(temp, Seq("courseid"), "right").select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
      , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
      "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dwd.dwd_sale_course.dt", "dwd.dwd_sale_course.dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }
}
