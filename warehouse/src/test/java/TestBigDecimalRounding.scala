import com.lyf.util.ParseJsonData

import scala.math.BigDecimal.RoundingMode

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/02 14:42
  * Version: 1.0
  */
object TestBigDecimalRounding {
  def main(args: Array[String]): Unit = {
    val json =
      """
{"chapter":"-","chapterid":0,"courseid":0,"createtime":"2019-07-22 09:08:52","creator":"admin","dn":"webA","dt":"20190722","excisenum":73,"modifystatus":"-","pointdescribe":"-","pointid":0,"pointlevel":"9","pointlist":"-","pointlistid":82,"pointname":"pointname0","pointnamelist":"-","pointyear":"2019","remid":"-","score":83.86880766562163,"sequece":"-","status":"-","thought":"-","typelist":"-"}
      """.stripMargin
    val obj = ParseJsonData.getJsonObject(json)

    //83.86880766562163
//    val decimal = BigDecimal.apply(obj.getDoubleValue("score"))
    val decimal = BigDecimal.apply("83.85000001")
    println(decimal)
    println("[四舍五入]res="+decimal.setScale(1, RoundingMode.HALF_UP))
    println("[=.85000时舍去，只要大于则进位]res="+decimal.setScale(1, RoundingMode.HALF_DOWN)) //针对中间值处理
    println("[down]res="+decimal.setScale(1, RoundingMode.DOWN))
    println("[up]res="+decimal.setScale(1, RoundingMode.UP))
  }
}
