package com.lyf.streaming.qz.process

import java.lang
import java.sql.{Connection, ResultSet}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.lyf.streaming.qz.util.{DbUtil, ParseJson, QueryCallback, SqlProxy}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/08 15:20
  * Version: 1.0
  * 手动维护offset
  */
object QzPointProcess {

  /**
   *  @author lyf3312
   *  @date 20/04/08 19:05
   *  @description 更新qz相关数据库表
   *
   */
  def qzQuestionUpdate(key: String, itemItr: Iterable[(String, String, String, String, String, String)], conn: Connection,sqlProxy: SqlProxy): Unit = {
    val keys = key.split("_")
    val userid = keys(0).toInt
    val courseid = keys(1).toInt
    val pointid = keys(2).toInt
    //将iterator转为list,进行多次操作
    val itemList = itemItr.toList
    //需求1:同一个用户做在同一门课程同一知识点下做题需要去重，并且需要记录去重后的做题id与个数。
    val questionId = itemList.map(_._4).distinct
    //需求2:计算知识点正确率 正确率计算公式：做题正确总个数(不去重)/做题总数(不去重) 保留两位小数
    //需求3:计算知识点掌握度 去重后的做题个数/当前知识点总题数（已知30题）*当前知识点的正确率

    //2.1 计算当前batch 的做题总个数(不去重)及做正确的题目个数(不去重)
    val qz_sum_curr = itemList.size
    val qz_istrue_curr = itemList.filter(_._5.equals("1")).size

    //2.2 从MySQL的`qz_point_detail`表中获取当前key对应历史做题总个数及正确总个数
    var qz_sum_history = 0
    var qz_istrue_history = 0
    var qz_count_history = 0
    val sql = "select * from `qz_point_detail` where userid=? and courseid=? and pointid=? "
    sqlProxy.executeQuery(conn, sql, Array(userid, courseid, pointid), new QueryCallback {
      override def process(resultSet: ResultSet): Unit = {
        while (resultSet.next()) {
          qz_sum_history = resultSet.getInt("qz_sum")
          qz_istrue_history = resultSet.getInt("qz_istrue")
          qz_count_history = resultSet.getInt("qz_count")
        }
        resultSet.close()
      }
    })

    //2.3 当前batch数据和历史数据进行合并,计算正确率
    val qz_sum_new = qz_sum_curr + qz_sum_history
    val qz_istrue_new = qz_istrue_curr + qz_istrue_history
    val correct_rate = qz_istrue_new.toDouble / qz_sum_new.toDouble
    val qz_count_new = questionId.size + qz_count_history

    //2.4 计算做题掌握度
    //2.4.1 从mysql `qz_point_history`中获取历史做题题目id(去重)
    var questionId_history: Array[String] = Array()
    val sql_point_history = "select * from `qz_point_history` where userid=? and courseid=? and pointid=?"
    sqlProxy.executeQuery(conn, sql_point_history, Array(userid, courseid, pointid), new QueryCallback {
      override def process(resultSet: ResultSet): Unit = {
        while (resultSet.next()) {
          questionId_history = resultSet.getString(1).split(",")
        }
        resultSet.close()
      }
    })
    //2.4.2 合并历史做题题目ids和当前做题题目ids
    val questionId_new = questionId.union(questionId_history).distinct
    val questionId_new_str = questionId_new.mkString(",")
    //2.4.3 计算掌握度,每个知识点的默认题目个数为30,(应当从数据库中获取)
    val mastery_rate = (qz_count_new.toDouble / 30) * correct_rate

    //2.5 获取其他字段
    val createtime_new = itemList.map(_._6).min //获取最早提交的时间作为当前uid_courseid_pointid的创建时间
    val updatetime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now())

    //2.6 将更新后的做题正确率及其他数据更新到`qz_point_detail` 和`qz_point_history`表中,供前端展示
    //2.6.1 更新`qz_point_history`
    val sql_update_point_history = "insert into qz_point_history(userid,courseid,pointid,questionids,createtime,updatetime) values(?,?,?,?,?,?) " +
      "on duplicate key update questionids=?,updatetime=?"
    sqlProxy.executeUpdate(conn,sql_update_point_history,Array(userid,courseid,pointid,questionId_new_str,createtime_new,createtime_new,questionId_new_str,updatetime))
    val sql_update_point_detail = "insert into qz_point_detail(userid,courseid,pointid,qz_sum,qz_count,qz_istrue,correct_rate,mastery_rate,createtime,updatetime) " +
      "values(?,?,?,?,?,?,?,?,?,?) on duplicate key update qz_sum=?,qz_count=?,qz_istrue=?,correct_rate=?,mastery_rate=?,updatetime=?"
    sqlProxy.executeUpdate(conn,sql_update_point_detail,Array(userid,courseid,pointid,qz_sum_new,qz_count_new,qz_istrue_new,correct_rate,mastery_rate,createtime_new,createtime_new,qz_sum_new,qz_count_new,qz_istrue_new,correct_rate,mastery_rate,updatetime))
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("QzProcess")//.setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "100") //设置各分区消费速率/s[仅对DirectStream流生效]
      .set("spark.streaming.backpressure.enabled","true")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    //设置topic及groupid
    val groupid = "qz_group"
    val topics = Array("qz_log")
    //保存topicPartition和offset值的map
    val offset = new mutable.HashMap[TopicPartition, Long]()

    //1.从mysql中获取消费offset
    val conn = DbUtil.getConnection
    val sqlProxy = new SqlProxy
    try {
      val sql_get_offset = "select * from `offset_manager` where groupid=? and topic=?"
      sqlProxy.executeQuery(conn, sql_get_offset, Array(groupid, topics(0)), new QueryCallback {
        override def process(resultSet: ResultSet): Unit = {
          //遍历数据,保存各分区的offset
          while (resultSet.next()) {
            val tp = new TopicPartition(resultSet.getString(2), resultSet.getInt("partition"))
            offset.put(tp, resultSet.getLong("untiloffset"))
          }
          resultSet.close()
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(conn)
    }

    //2.根据offset,从kafka中消费数据,获取原始Stream,并进行简单过滤,转为qzPointStream
    val params = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest", //从头开始消费
      "enable.auto.commit" -> (false: lang.Boolean) //取消自动维护偏移量
    )
    val stream = if (offset.isEmpty) {
      KafkaUtils.createDirectStream(ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, params)
      )
    } else {
      KafkaUtils.createDirectStream(ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, params, offset)
      )
    }
    val qzPointStream = stream.filter(cr => cr.value().split("\t").length == 6)
      .mapPartitions(partition => {
        partition.map(cr => {
          val fields = cr.value().split("\t")
          val userid = fields(0) //用户id
          val courseid = fields(1)
          //课程id
          val pointid = fields(2) //知识点id
          val questionid = fields(3) //题目id
          val istrue = fields(4) //答案是否正确
          val createtime = fields(5) //创建时间
          (userid, courseid, pointid, questionid, istrue, createtime)
        })
      })

    //3.转换数据,完成业务需求
    qzPointStream.foreachRDD(rdd => {
      //3.1为避免多线程安全问题,将原始数据在driver端按uid_courseid_pointid进行分组
      val groupRdd = rdd.groupBy(item => item._1 + "_" + item._2 + "_" + item._3)
      //3.2 遍历各分区数据,计算指标
      groupRdd.foreachPartition(itr => {
        //在executor上执行
        val conn = DbUtil.getConnection
        val sqlProxy = new SqlProxy
        //注: 此时的
        //对分区内每条数据进行操作,计算指标
        try {
          itr.foreach {
            case (key, itemItr) => {
              //计算同一用户,同一门课程,同一知识点下各指标,更新做题相关数据库,
              qzQuestionUpdate(key, itemItr, conn,sqlProxy)
            }
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(conn)
        }
      })
    })


    //4.保存消费offset到mysql
    stream.foreachRDD(rdd => {
      val conn = DbUtil.getConnection
      val sqlProxy = new SqlProxy
      try {
        val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val sql_update_offset = "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)"
        for (elem <- ranges) {
          sqlProxy.executeUpdate(conn,
            sql_update_offset,
            Array(groupid, elem.topic, elem.partition, elem.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(conn)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
