import com.lyf.util.ParseJsonData

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/02 14:30
  * Version: 1.0
  */
object testGetTimeStamp {
  def main(args: Array[String]): Unit = {
    val json =
      """
{"chapterid":0,"chapterlistid":0,"chaptername":"chaptername0","chapternum":10,"courseid":61,"createtime":"2019-07-22 16:37:24","creator":"admin","dn":"webA","dt":"20190722","outchapterid":0,"sequence":"-","showstatus":"-","status":"-"}
    """.stripMargin
    val obj = ParseJsonData.getJsonObject(json)
    val timestamp = obj.getTimestamp("createtime")
    println(timestamp)

    println("string:"+obj.getString("createtime"))
  }
}
