package com.lyf.qz.service

import com.lyf.qz.dao.AdsQzDao
import org.apache.spark.sql.SparkSession

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/02 16:31
  * Version: 1.0
  */
object AdsQzService {

  def getMetricsWithSQL(ss: SparkSession, dt: String): Unit = {
    AdsQzDao.getAvgSPendTimeAndScore(ss,dt)
    AdsQzDao.getQuestionDetail(ss,dt)
    AdsQzDao.getPaperPassDetail(ss,dt)
    AdsQzDao.getLow3UserDetail(ss,dt)
    AdsQzDao.getTop3UserDetail(ss,dt)
    AdsQzDao.getTopScore(ss,dt)
    AdsQzDao.getPaperScoreSegmentUser(ss,dt)
  }

  //采用DSL风格，实现指标查询
  def getMetricsWithApi(ss: SparkSession, dt: String): Unit = {

  }
}
