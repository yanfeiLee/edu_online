package com.lyf.streaming.qz.process

import java.lang
import java.sql.ResultSet

import com.lyf.streaming.qz.util.{DbUtil, QueryCallback, SqlProxy}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/07 19:28
  * Version: 1.0
  * 保证了数据不丢失,但存在重复消费问题
  */
object RegisterProcess {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RegisterProcess").setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "100") //设置每个分区的发送速率*10个分区==>每秒消费1000条数据
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    //从kafka读数据,手动维护偏移量
    val topics = Array("register_topic") //可以同时从多个topic中consumer数据
    //topic所属组id
    val groupid = "register_group"
    //存储offset
    val offset = new mutable.HashMap[TopicPartition,Long]()

    //1.从mysql中查询偏移量,
    val sqlProxy = new SqlProxy
    val conn = DbUtil.getConnection
    val sql = "select * from `offset_manager` where groupid=?"
    try{
      sqlProxy.executeQuery(conn,sql,Array(groupid),new QueryCallback {
        override def process(resultSet: ResultSet): Unit = {
          //将db中保存的当前group ,topic 和partition的offset取出
            while (resultSet.next()){
                val tp = new TopicPartition(resultSet.getString(2),resultSet.getInt(3))
                offset.put(tp,resultSet.getLong(4))
            }
          //关闭游标
          resultSet.close()
        }
      })
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      sqlProxy.shutdown(conn)
    }
    //2从指定offset处开始消费数据
      //连接kafka参数
      val params: Map[String, Object] = Map[String, Object](
        "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> groupid,
        "auto.offset.reset" -> "earliest",   //sparkstreaming第一次启动，不丢数
        //如果是true，则这个消费者的偏移量会在后台自动提交，但是kafka宕机容易丢失数据
        //如果是false，则需要手动维护kafka偏移量
        "enable.auto.commit" -> (false: lang.Boolean)
      )
      //创建kafka消费者
      val ids = if(offset.isEmpty){
        //从头开始消费
          KafkaUtils.createDirectStream[String, String](
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](topics, params)
        )
      }else{
        //从之前消费的位置开始消费
        KafkaUtils.createDirectStream[String,String](
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String,String](topics,params,offset)
        )
      }

    //3数据转换得出指标
    //解析出需要的字段
    val fromStream = ids.filter(item => item.value().split("\t").length == 3)
      .mapPartitions(partition => {
        partition.map(item => {
          val line = item.value()
          val fields = line.split("\t")
          val fromName = fields(1) match {
            case "1" => "PC"
            case "2" => "APP"
            case _ => "Other"
          }
          (fromName, 1)
        })
      })
    //sparkStreaming对有状态的数据操作，需要设定检查点目录，然后将状态保存到检查点中
    ssc.checkpoint("/user/lee/sparkstreaming/checkpoint") //会产生大量小文件
    fromStream.cache() //后续分流做各个需求
    //需求1:实时统计注册人数，批次为3秒一批
    fromStream.updateStateByKey(
      //匿名函数,对历史数据和当前数据进行处理
      //value为本批次数据
      //state为历史数据计算结果
      (value:Seq[Int],state:Option[Int])=>{
      Some(value.sum+state.getOrElse(0))
    }
    ).print()
    //需求2:每6秒统统计一次1分钟内的注册数据，不需要历史数据
    //fromStream.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Minutes(1),Seconds(6)).print()

    //4将消费数据后offset提交保存到mysql
    //从ids中获取offset
    ids.foreachRDD(rdd=>{
      val sqlProxy = new SqlProxy
      val conn = DbUtil.getConnection
      val sql = "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)"
      try{
        val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- ranges) {
          //将各个分区offset数据写入mysql
          sqlProxy.executeUpdate(
            conn,
            sql,
            Array(groupid,or.topic,or.partition,or.untilOffset)
          )
        }
      }catch{
        case e:Exception=>e.printStackTrace()
      }finally {
        sqlProxy.shutdown(conn)
      }
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
