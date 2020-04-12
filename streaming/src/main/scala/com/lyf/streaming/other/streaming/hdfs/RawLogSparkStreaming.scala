package com.lyf.streaming.other.streaming.hdfs

import java.lang
import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import com.lyf.streaming.qz.util.{DbUtil, OffsetUtil, QueryCallback, SqlProxy}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/12 9:46
  * Version: 1.0
  */
object RawLogSparkStreaming {
  private var fs: FileSystem = null
  private var fSOutputStream: FSDataOutputStream = null
  private var writePath: Path = null
  private val hdfsBasicPath = "hdfs://nameservice1/user/lee/rawlogdata/"

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("RawLogSparkStreaming")
      .set("spark.streaming.kafka.maxRatePerPartition", "20") //设置消费速率
      .set("spark.streaming.backpressure.enabled", "true") //启用背压机制
      .set("spark.streaming.stopGracefullyOnShutdown", "true") //启用平滑关闭
//      .setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //0.初始化groupid及topic
    val groupid = "page_group"
    val topics = Array("page_topic")
    //1.从mysql中获取偏移量
    val offset = OffsetUtil.getOffset(groupid,topics)
    //2.创建原始数据流
    val params = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest", //从头开始消费
      "enable.auto.commit" -> (false: lang.Boolean) //取消自动维护偏移量
    )
    val dataStream = if (offset.isEmpty){
      //从头开始消费
      KafkaUtils.createDirectStream[String,String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](topics,params)
      )
    }else{
      //从指定位置开始消费
      KafkaUtils.createDirectStream[String,String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](topics,params,offset)
      )
    }

    //3.将流数据写入到hdfs
    val kvStream = dataStream.map(item=>(item.key(),item.value()))
    kvStream.foreachRDD(rdd=>{
      val conf = new JobConf()
      conf.set("mapred.output.compress", "true")
      conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec") //采用snappy压缩
      val writePath = getTotalPath(System.currentTimeMillis(),topics(0))
      //因为rdd.saveAsHadoopFile这个方法存在PairRDDFunctions类中，所以要提前将DStream转成（K,V）的形式
      rdd.saveAsHadoopFile(writePath,classOf[Text],classOf[Text],classOf[RDDMultipleAppendTextOutputFormat],conf)
    })

    //4.持久化offset 到mysql
    OffsetUtil.setOffset(groupid,dataStream)

    ssc.start()
    ssc.awaitTermination()
  }

  //将时间戳格式化到天获取完整路径,达到一天一个路径的效果
  def getTotalPath(lastTime: Long,topic:String): String = {
    val dft = DateTimeFormatter.ofPattern("yyyyMMdd")
    val formatDate = dft.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(lastTime), ZoneId.systemDefault()))
    //val directories = formatDate.split("-")
    val totalPath = hdfsBasicPath + "/" + topic + "/" + formatDate
    totalPath
  }
}
