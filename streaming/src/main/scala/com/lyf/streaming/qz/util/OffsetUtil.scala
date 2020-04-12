package com.lyf.streaming.qz.util

import java.io.DataInputStream
import java.sql.ResultSet

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.HasOffsetRanges

import scala.collection.mutable

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/12 9:51
  * Version: 1.0
  */
object OffsetUtil {
  /**
   *  @author lyf3312
   *  @date 20/04/12 10:14
   *  @description 获取指定groupid和topic的偏移量
   *
   */
  def getOffset(groupid: String, topics: Array[String]) = {
    val offset = new mutable.HashMap[TopicPartition, Long]()
    val proxy = new SqlProxy
    val conn = DbUtil.getConnection
    val res = try {
      val sql_offset_query = "select * from `offset_manager` where groupid=? and topic=?"
      proxy.executeQuery(conn, sql_offset_query, Array(groupid, topics(0)), new QueryCallback {
        override def process(resultSet: ResultSet): Unit = {
          while (resultSet.next()) {
            val tp = new TopicPartition(resultSet.getString("topic"), resultSet.getInt("partition"))
            offset.put(tp, resultSet.getLong("untiloffset"))
          }
          resultSet.close()
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      proxy.shutdown(conn)
    }
    offset
  }

  /**
   *  @author lyf3312
   *  @date 20/04/12 11:09
   *  @description 保存消费后的offset
   *
   */
  def setOffset(groupid:String,dataStream: InputDStream[ConsumerRecord[String, String]]): Unit ={
    dataStream.foreachRDD(rdd=>{
      val proxy = new SqlProxy
      val conn = DbUtil.getConnection
      try {
        val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (elem <- ranges) {
          proxy.executeUpdate(conn,"replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?) ",
            Array(groupid,elem.topic,elem.partition,elem.untilOffset))
        }
      }catch {
        case e:Exception=>e.printStackTrace()
      }finally {
        proxy.shutdown(conn)
      }
    })
  }
}
