package com.lyf.qz.dao

import org.apache.spark.sql.SparkSession

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/02 16:07
  * Version: 1.0
  */
object QzMajorDao {
  def getQzMajor(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select majorid,businessid,siteid,majorname,shortname,status,sequence,creator as major_creator," +
      s"createtime as major_createtime,dt,dn from dwd.dwd_qz_major where dt='$dt'")
  }

  def getQzWebsite(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select siteid,sitename,domain,multicastserver,templateserver,creator," +
      s"createtime,multicastgateway,multicastport,dn from dwd.dwd_qz_website where dt='$dt'")
  }

  def getQzBusiness(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select businessid,businessname,dn from dwd.dwd_qz_business where dt='$dt'")
  }
}
