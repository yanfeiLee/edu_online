/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/12 10:08
  * Version: 1.0
  */
object trycatchReturn {
  def main(args: Array[String]): Unit = {
   testTry()
  }
  def testTry() ={
    var s="ok"
    val res = try {
      val i = 10/0
      s
    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      s.concat("-no-")
    }
    println(res)
  }
}
