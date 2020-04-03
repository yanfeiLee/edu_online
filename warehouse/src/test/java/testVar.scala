/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/02 15:53
  * Version: 1.0
  */
object testVar {
  def main(args: Array[String]): Unit = {
    val dt = "20201122"
    val margin =
      s"""
          select chapterid,
                 chapterlistid,
                 chaptername,
                 sequence,
                 showstatus,
                 creator as chapter_creator,
                 createtime as chapter_createtime,
                 courseid as chapter_courseid,
                 chapternum,outchapterid,
                 dt,
                 dn
          from dwd.dwd_qz_chapter
          where dt='$dt'
      """.stripMargin
    println(margin)
  }
}
