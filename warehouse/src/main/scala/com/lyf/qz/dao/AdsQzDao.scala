package com.lyf.qz.dao

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/02 17:39
  * Version: 1.0
  */
object AdsQzDao {

  //统计各试卷平均耗时及平均分
  def getAvgSPendTimeAndScore(ss: SparkSession, dt: String): Unit = {
    ss.sql(
      s"""
         |select
         |    paperviewid,
         |    paperviewname,
         |    cast(avg(score) as decimal(4,1)) score,
         |    cast(avg(spendtime) as decimal(10,2)) spendtime,
         |    dt,
         |    dn
         |from dws.dws_user_paper_detail
         |where dt='${dt}'
         |group by paperviewid,paperviewname,dn,dt
         |order by score desc,spendtime desc
       """.stripMargin)
      .coalesce(3)
      .write.mode(SaveMode.Overwrite)
      .insertInto("ads.ads_paper_avgtimeandscore")
  }

  /**
    * 统计试卷 最高分 最低分
    *
    * @param sparkSession
    * @param dt
    */
  def getTopScore(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""select
         |    paperviewid,
         |    paperviewname,
         |    cast(max(score) as decimal(4,1)),
         |    cast(min(score) as decimal(4,1)),
         |    dt,
         |    dn
         |from dws.dws_user_paper_detail
         |where dt='$dt'
         |group by paperviewid,paperviewname,dt,dn""".stripMargin)
      .coalesce(3).write.mode(SaveMode.Append).insertInto("ads.ads_paper_maxdetail")
  }

  /**
    * 按试卷分组获取每份试卷的分数前三用户详情
    *
    * @param sparkSession
    * @param dt
    */
  def getTop3UserDetail(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""select *
         |from
         |    (
         |    select
         |        userid,paperviewid,paperviewname,chaptername,pointname,
         |        sitecoursename,coursename,majorname,
         |        shortname,papername,score,
         |        dense_rank() over (partition by paperviewid order by score desc) as rk,
         |        dt,dn
         |    from dws.dws_user_paper_detail
         |    )
         |where rk<4""".stripMargin)
      .coalesce(3).write.mode(SaveMode.Append).insertInto("ads.ads_top3_userdetail")
  }

  /**
    * 按试卷分组获取每份试卷的分数倒数三的用户详情
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getLow3UserDetail(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""select *
         |from
         |    (
         |     select
         |        userid,paperviewid,paperviewname,chaptername,pointname,
         |        sitecoursename,coursename,majorname,
         |        shortname,papername,score,
         |        dense_rank() over (partition by paperviewid order by score asc) as rk,
         |        dt,dn
         |     from dws.dws_user_paper_detail
         |     where dt='$dt'
         |    )
         |where rk<4""".stripMargin)
      .coalesce(3).write.mode(SaveMode.Append).insertInto("ads.ads_low3_userdetail")
  }

  //统计各试卷各分段的用户id，分段有0-20,20-40,40-60,60-80,80-100
  def getPaperScoreSegmentUser(sparkSession: SparkSession, dt: String): Unit = {
    sparkSession.sql(
      s"""
         |select
         |    paperviewid,paperviewname,score_segment,
         |    concat_ws(',',collect_list(cast(userid as string))),
         |    dt,dn
         |from(
         |    select
         |          paperviewid,paperviewname,userid,
         |          case   when score >=0  and score <=20 then '0-20'
         |                 when score >20 and score <=40 then '20-40'
         |                 when score >40 and score <=60 then '40-60'
         |                 when score >60 and score <=80 then '60-80'
         |                 when score >80 and score <=100 then '80-100'
         |          end as score_segment,
         |          dt,dn
         |    from dws.dws_user_paper_detail
         |    where dt='${dt}'
         |  )
         |group by paperviewid,paperviewname,score_segment,dt,dn
         |order by paperviewid,score_segment
       """.stripMargin)
      .coalesce(3)
      .write.mode(SaveMode.Overwrite)
      .insertInto("ads.ads_paper_scoresegment_user")
  }

  //统计试卷未及格的人数，及格的人数，试卷的及格率 及格分数60
  def getPaperPassDetail(sparkSession: SparkSession,dt:String): Unit ={
    sparkSession.sql(
      s"""
         |select
         |    t.paperviewid,t.paperviewname,t.countdetail,t.passcount,
         |    cast(t.passcount/(t.passcount+t.countdetail) as decimal(4,2)) as rate,
         |    t.dt,t.dn
         |from
         |    (
         |     select a.paperviewid,a.paperviewname,a.countdetail,a.dt,a.dn,b.passcount
         |     from
         |        (
         |          select paperviewid,paperviewname,count(*) countdetail,dt,dn
         |          from dws.dws_user_paper_detail
         |          where dt='$dt' and score between 0 and 60
         |          group by paperviewid,paperviewname,dt,dn
         |        ) a
         |        join
         |        (
         |          select paperviewid,count(*) passcount,dn
         |          from dws.dws_user_paper_detail
         |          where dt='$dt' and score >60
         |          group by paperviewid,dn
         |        ) b
         |        on a.paperviewid=b.paperviewid and a.dn=b.dn
         |    )t
       """.stripMargin)
      .coalesce(3)
      .write.mode(SaveMode.Overwrite)
      .insertInto("ads.ads_user_paper_detail")
  }

  /**
    * 统计各题 正确人数 错误人数 错题率
    *
    * @param sparkSession
    * @param dt
    */
  def getQuestionDetail(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"""select
                        |    t.questionid,t.errcount,t.rightcount,
                        |    cast(t.errcount/(t.errcount+t.rightcount) as decimal(4,2)) as rate,
                        |    t.dt,t.dn
                        |from
                        |    (
                        |     select a.questionid,a.errcount,b.rightcount,a.dt,a.dn
                        |     from
                        |      (
                        |        select questionid,count(*) errcount,dt,dn
                        |        from dws.dws_user_paper_detail
                        |        where dt='$dt' and user_question_answer='0'
                        |        group by questionid,dt,dn
                        |      ) a
                        |      join
                        |      (
                        |        select questionid,count(*) rightcount,dt,dn
                        |        from dws.dws_user_paper_detail
                        |        where dt='$dt' and user_question_answer='1'
                        |        group by questionid,dt,dn
                        |      ) b
                        |      on a.questionid=b.questionid and a.dn=b.dn
                        |    ) t
                        |order by errcount desc""".stripMargin)
      .coalesce(3)
      .write.mode(SaveMode.Append)
      .insertInto("ads.ads_user_question_detail")
  }

}
