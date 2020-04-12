import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/08 17:32
  * Version: 1.0
  */
object testEq {
  def main(args: Array[String]): Unit = {
//    println("1".eq("1"))
//    println(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()))
//    val str = "hello_abc_23"
//    val ss = str.split("_")
//    println(ss(0))
//    println(ss(1))
//    println(ss(2))

//    val list = List((1,"1"),(2,"0"),(3,"1"),(4,"0"))
//    println(list.filter(_._2.eq("1")).size)

    //流中字符串的eq比较
//    val conf: SparkConf = new SparkConf().setAppName("testEq").setMaster("local[*]")
//    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
//
//    val source = ssc.socketTextStream("hadoop102",4040)
//    source.filter(_.equals("a")).print() //scala中equals 和== 一样都是比较字符是否相等
//    //stream中字符地址,和本地创建一个相同字符,地址不相等
//    source.filter(_.eq("bb")).print()  //scala中eq比较对象地址是否相等
//
//    ssc.start()
//    ssc.awaitTermination()

    val obj1 = null;
    val obj2 = null
    val obj3 = new Stu
    val obj4 = new Stu

    println(obj1.eq(obj2))
    println(obj1 == obj2)

//    println(obj1.equals(obj2)) //NullPointerException
    println("----------------------------------------------")
    println(obj3.eq(obj4))
    println(obj3 == obj4)
  }
}
class Stu{

}
