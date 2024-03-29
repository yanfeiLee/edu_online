package com.lyf.qz.service

import com.lyf.qz.dao._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/02 15:11
  * Version: 1.0
  */
object DwdQzService {

  //章节降维表
  def saveDwsQzChapter(ss: SparkSession, dt: String): Unit = {
    val dwdQzChapter = QzChapterDao.getDwdQzChapter(ss, dt)
    val dwdQzChapterlist = QzChapterDao.getDwdQzChapterList(ss, dt)
    val dwdQzPoint = QzChapterDao.getDwdQzPoint(ss, dt)
    val dwdQzPointQuestion = QzChapterDao.getDwdQzPointQuestion(ss, dt)

    //      dwdQzChapter.join(dwdQzChapterlist,dwdQzChapter("chapterlistid") === dwdQzChapterlist("chapterlistid")&&dwdQzChapter("dn") === dwdQzChapterlist("dn")) //连接的列不会去重
    //此join会对连接的column进行去重，
    val res = dwdQzChapter.join(dwdQzChapterlist, Seq("chapterlistid", "dn")) //默认为内连接
      .join(dwdQzPoint, Seq("chapterid", "dn"), "inner")
      .join(dwdQzPointQuestion, Seq("pointid", "dn"))
    //过滤连接后，需要的字段
    res.select("chapterid", "chapterlistid", "chaptername", "sequence", "showstatus", "showstatus",
      "chapter_creator", "chapter_createtime", "chapter_courseid", "chapternum", "chapterallnum", "outchapterid", "chapterlistname",
      "pointid", "questionid", "questype", "pointname", "pointyear", "chapter", "excisenum", "pointlistid", "pointdescribe",
      "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist", "typelistids", "pointlist", "dt", "dn")
      .coalesce(2)
      .write.mode(SaveMode.Overwrite) //overwrite或append取决于业务需求
      .insertInto("dws.dws_qz_chapter")
  }

  //课程降维表
  def saveDwsQzCourse(sparkSession: SparkSession, dt: String) = {
    val dwdQzSiteCourse = QzCourseDao.getDwdQzSiteCourse(sparkSession, dt)
    val dwdQzCourse = QzCourseDao.getDwdQzCourse(sparkSession, dt)
    val dwdQzCourseEdusubject = QzCourseDao.getDwdQzCourseEduSubject(sparkSession, dt)
    val result = dwdQzSiteCourse.join(dwdQzCourse, Seq("courseid", "dn"))
      .join(dwdQzCourseEdusubject, Seq("courseid", "dn"))
      .select("sitecourseid", "siteid", "courseid", "sitecoursename", "coursechapter",
        "sequence", "status", "sitecourse_creator", "sitecourse_createtime", "helppaperstatus", "servertype", "boardid",
        "showstatus", "majorid", "coursename", "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid"
        , "dt", "dn")
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_course")
  }

  //专业降维表
  def saveDwsQzMajor(sparkSession: SparkSession, dt: String) = {
    val dwdQzMajor = QzMajorDao.getQzMajor(sparkSession, dt)
    val dwdQzWebsite = QzMajorDao.getQzWebsite(sparkSession, dt)
    val dwdQzBusiness = QzMajorDao.getQzBusiness(sparkSession, dt)
    val result = dwdQzMajor.join(dwdQzWebsite, Seq("siteid", "dn"))
      .join(dwdQzBusiness, Seq("businessid", "dn"))
      .select("majorid", "businessid", "siteid", "majorname", "shortname", "status", "sequence",
        "major_creator", "major_createtime", "businessname", "sitename", "domain", "multicastserver", "templateserver",
        "multicastgateway", "multicastport", "dt", "dn")
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_major")
  }

  //做题降维表
  def saveDwsQzPaper(sparkSession: SparkSession, dt: String) = {
    val dwdQzPaperView = QzPaperDao.getDwdQzPaperView(sparkSession, dt)
    val dwdQzCenterPaper = QzPaperDao.getDwdQzCenterPaper(sparkSession, dt)
    val dwdQzCenter = QzPaperDao.getDwdQzCenter(sparkSession, dt)
    val dwdQzPaper = QzPaperDao.getDwdQzPaper(sparkSession, dt)
    val result = dwdQzPaperView.join(dwdQzCenterPaper, Seq("paperviewid", "dn"), "left")
      .join(dwdQzCenter, Seq("centerid", "dn"), "left")
      .join(dwdQzPaper, Seq("paperid", "dn"))
      .select("paperviewid", "paperid", "paperviewname", "paperparam", "openstatus", "explainurl", "iscontest"
        , "contesttime", "conteststarttime", "contestendtime", "contesttimelimit", "dayiid", "status", "paper_view_creator",
        "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse", "paperdifficult", "testreport",
        "paperuseshow", "centerid", "sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
        "stage", "papercatid", "courseid", "paperyear", "suitnum", "papername", "totalscore", "chapterid", "chapterlistid",
        "dt", "dn")
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_paper")
  }

  //做题问题表
  def saveDwsQzQuestionTpe(sparkSession: SparkSession, dt: String) = {
    val dwdQzQuestion = QzQuestionDao.getQzQuestion(sparkSession, dt)
    val dwdQzQuestionType = QzQuestionDao.getQzQuestionType(sparkSession, dt)
    val result = dwdQzQuestion.join(dwdQzQuestionType, Seq("questypeid", "dn"))
      .select("questionid", "parentid", "questypeid", "quesviewtype", "content", "answer", "analysis"
        , "limitminute", "score", "splitscore", "status", "optnum", "lecture", "creator", "createtime", "modifystatus"
        , "attanswer", "questag", "vanalysisaddr", "difficulty", "quesskill", "vdeoaddr", "viewtypename", "papertypename",
        "remark", "splitscoretype", "dt", "dn")
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_question")
  }


  //用户做题详情大宽表
  def saveDwsUserPaperDetail(sparkSession: SparkSession,dt:String): Unit ={
    //dwd层用户做题详情表
    val dwdQzMemberPaperQuestion = UserPaperDetailDao.getDwdQzMemberPaperQuestion(sparkSession, dt).drop("paperid")
      .withColumnRenamed("question_answer", "user_question_answer")
   //dws层降维表
    val dwsQzChapter = UserPaperDetailDao.getDwsQzChapter(sparkSession, dt).drop("courseid")
    val dwsQzCourse = UserPaperDetailDao.getDwsQzCourse(sparkSession, dt).withColumnRenamed("sitecourse_creator", "course_creator")
      .withColumnRenamed("sitecourse_createtime", "course_createtime").drop("majorid")
      .drop("chapterlistid").drop("pointlistid")
    val dwsQzMajor = UserPaperDetailDao.getDwsQzMajor(sparkSession, dt)
    val dwsQzPaper = UserPaperDetailDao.getDwsQzPaper(sparkSession, dt).drop("courseid")
    val dwsQzQuestion = UserPaperDetailDao.getDwsQzQuestion(sparkSession, dt)

    dwdQzMemberPaperQuestion.join(dwsQzCourse, Seq("sitecourseid", "dn")).
      join(dwsQzChapter, Seq("chapterid", "dn")).join(dwsQzMajor, Seq("majorid", "dn"))
      .join(dwsQzPaper, Seq("paperviewid", "dn")).join(dwsQzQuestion, Seq("questionid", "dn"))
      .select("userid", "courseid", "questionid", "useranswer", "istrue", "lasttime", "opertype",
        "paperid", "spendtime", "chapterid", "chaptername", "chapternum",
        "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questype", "pointyear", "chapter", "pointname"
        , "excisenum", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist",
        "typelistids", "pointlist", "sitecourseid", "siteid", "sitecoursename", "coursechapter", "course_sequence", "course_status"
        , "course_creator", "course_createtime", "servertype", "helppaperstatus", "boardid", "showstatus", "majorid", "coursename",
        "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "businessid", "majorname", "shortname",
        "major_status", "major_sequence", "major_creator", "major_createtime", "businessname", "sitename",
        "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport", "paperviewid", "paperviewname", "paperparam",
        "openstatus", "explainurl", "iscontest", "contesttime", "conteststarttime", "contestendtime", "contesttimelimit",
        "dayiid", "paper_status", "paper_view_creator", "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse",
        "testreport", "centerid", "paper_sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
        "paper_stage", "papercatid", "paperyear", "suitnum", "papername", "totalscore", "question_parentid", "questypeid",
        "quesviewtype", "question_content", "question_answer", "question_analysis", "question_limitminute", "score",
        "splitscore", "lecture", "question_creator", "question_createtime", "question_modifystatus", "question_attanswer",
        "question_questag", "question_vanalysisaddr", "question_difficulty", "quesskill", "vdeoaddr", "question_description",
        "question_splitscoretype", "user_question_answer", "dt", "dn").coalesce(1)
      .write.mode(SaveMode.Append).insertInto("dws.dws_user_paper_detail")
  }
}
