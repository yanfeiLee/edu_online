import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/10 9:47
  * Version: 1.0
  */
object TestIp2Region {
  def main(args: Array[String]): Unit = {
    val searcher = new DbSearcher(new DbConfig(),this.getClass.getResource("/ip2region.db").getPath)
    val block = searcher.binarySearch("121.11.103.127").getRegion
    println(block)
  }
}
