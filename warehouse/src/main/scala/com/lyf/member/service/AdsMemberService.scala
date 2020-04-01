package com.lyf.member.service

import com.lyf.common.Constant
import com.lyf.member.dao.AdsMemberDao
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/01 20:14
  * Version: 1.0
  */
object AdsMemberService {

  def queryDetail(ss: SparkSession, dt: String): Unit = {
    AdsMemberDao.queryAppregurlCount(ss, dt)
    AdsMemberDao.queryAdNameCount(ss, dt)
    AdsMemberDao.queryMemberLevelCount(ss, dt)
    AdsMemberDao.queryRegsourceNameCount(ss, dt)
    AdsMemberDao.queryVipLevelCount(ss,dt)
    AdsMemberDao.getTop3MemberLevelPayMoneyUser(ss,dt)
    AdsMemberDao.querySiteNameCount(ss,dt)

  }
}
