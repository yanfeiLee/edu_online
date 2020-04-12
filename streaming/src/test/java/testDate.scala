import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/12 11:28
  * Version: 1.0
  */
object testDate {
  def main(args: Array[String]): Unit = {
    val dft = DateTimeFormatter.ofPattern("yyyyMMdd")
    val localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()),ZoneId.systemDefault())
    val ldt = LocalDateTime.now()
    val str = dft.format(ldt)
    println(str)
    println("----------------------------------------------")
    println(dft.format(localDateTime))
    println(LocalDateTime.now())
  }
}
