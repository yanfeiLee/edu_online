package com.lyf.streaming.qz.process

import java.lang
import java.sql.{Connection, ResultSet}
import java.text.NumberFormat

import com.alibaba.fastjson.JSONObject
import com.lyf.streaming.qz.util._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/10 18:58
  * Version: 1.0
  */
object PageProcess {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("PageProcess")
      .set("spark.streaming.kafka.maxRatePerPartition", "100") //设置消费速率
      .set("spark.streaming.backpressure.enabled", "true") //启用背压机制
      .set("spark.streaming.stopGracefullyOnShutdown", "true") //启用平滑关闭
//      .setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //0.初始化groupid,topic
    val groupid = "page_group"
    val topics = Array("page_topic")


    val offset = new mutable.HashMap[TopicPartition, Long]()
    //1.从mysql获取消费偏移量offset
    val proxy = new SqlProxy
    val conn = DbUtil.getConnection
    try {
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
    //2.根据offset获取原始流,并转换为指定类型流
    val params = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest", //从头开始消费
      "enable.auto.commit" -> (false: lang.Boolean) //取消自动维护偏移量
    )
    val dataStream = if (offset.isEmpty) {
      KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, params)
      )
    } else {
      KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, params, offset)
      )
    }
    val pageStream = dataStream.filter(item => {
      val jsonString = item.value()
      ParseJson.getJsonObj(jsonString).isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(cr => {
        val jsonStr = cr.value()
        val jsonObject = ParseJson.getJsonObj(jsonStr)
        val uid = if (jsonObject.containsKey("uid")) jsonObject.getString("uid") else ""
        val app_id = if (jsonObject.containsKey("app_id")) jsonObject.getString("app_id") else ""
        val device_id = if (jsonObject.containsKey("device_id")) jsonObject.getString("device_id") else ""
        val ip = if (jsonObject.containsKey("ip")) jsonObject.getString("ip") else ""
        val last_page_id = if (jsonObject.containsKey("last_page_id")) jsonObject.getString("last_page_id") else ""
        val pageid = if (jsonObject.containsKey("page_id")) jsonObject.getString("page_id") else ""
        val next_page_id = if (jsonObject.containsKey("next_page_id")) jsonObject.getString("next_page_id") else ""
        (uid, app_id, device_id, ip, last_page_id, pageid, next_page_id)
      })
    }).filter(item => {
      !item._5.equals("") && !item._6.equals("") && !item._7.equals("")
    })
    pageStream.cache() //后续分流进行计算

    //3.进行流转换,完成需求
    //需求一:计算商品课程页总浏览数、订单页总浏览数、支付页面总浏览数
    val countResStream = pageStream.mapPartitions(partition => {
      partition.map(item => (item._5 + "_" + item._6 + "_" + item._7, 1))
    }).reduceByKey(_ + _)
    //将计算结果写入到MySQL
    countResStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val proxy = new SqlProxy
        val conn = DbUtil.getConnection
        try {
          //计算需求一的指标
          partition.foreach(item => {
            calPageCount(proxy, conn, item)
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          proxy.shutdown(conn)
        }
      })
    })

    //需求三:根据ip得出相应省份，展示出top3省份的点击数，需要根据历史数据累加
    //广播ip2region文件到各executor
    ssc.sparkContext.addFile("hdfs://nameservice1/user/lee/sparkstreaming/ip2region.db")
    //转换流结构,对各区域ip进行聚合
    val top3Stream = pageStream.mapPartitions(partition => {
      val dbFile = SparkFiles.get("ip2region.db")
      val searcher = new DbSearcher(new DbConfig(), dbFile)
      partition.map(item => {
        val ip = item._4
        val province = searcher.memorySearch(ip).getRegion.split("\\|")(2)
        (province, 1l)
      })
    }).reduceByKey(_ + _)

    //存储top3到mysql
    top3Stream.foreachRDD(rdd => {
      //1)从MySQL tmp_city_num_detail表获取历史数据(各省份pv)
      val ipProxy = new SqlProxy
      val ipConn = DbUtil.getConnection
      try {
        val history_click_num = new ArrayBuffer[(String, Long)]()
        ipProxy.executeQuery(ipConn, "select province,num from tmp_city_num_detail", null, new QueryCallback {
          override def process(resultSet: ResultSet): Unit = {
            while (resultSet.next()) {
              history_click_num.append((resultSet.getString("province"), resultSet.getLong("num")))
            }
          }
        })
        //2)将histroy_click_num 转为rdd,与当前批次数据进行join
        val rdd_history_clickNum = ssc.sparkContext.makeRDD(history_click_num)
        val resRdd = rdd.fullOuterJoin(rdd_history_clickNum)
          .map(item => {
            (item._1, item._2._1.getOrElse(0l) + item._2._2.getOrElse(0l))
          })

        //3)取top3 插入到mysql
        val topRdd = resRdd.sortBy[Long](_._2,false).take(3)
        //清空表
        ipProxy.executeUpdate(ipConn,"truncate table top_city_num",null)
        //写入新的topN
        topRdd.foreach(item=>{
          ipProxy.executeUpdate(ipConn,"insert into top_city_num (province,num) values(?,?) ",Array(item._1,item._2))
        })

        //4)将最新provinc num数据写入mysql tmp_city_num_detail表
        resRdd.foreachPartition(partition => {
          val proxy = new SqlProxy
          val conn = DbUtil.getConnection
          try {
            partition.foreach(item=>{
              val province = item._1
              val num = item._2
              proxy.executeUpdate(conn,"insert into tmp_city_num_detail(province,num) values(?,?) on duplicate key update num=? ",Array(province,num,num) )
            })
          }catch {
            case e:Exception=>e.printStackTrace()
          }finally {
            proxy.shutdown(conn)
          }
        })
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
       ipProxy.shutdown(ipConn)
      }
    })


    //4.提交偏移量,同时完成需求二的计算
    dataStream.foreachRDD(rdd => {
      val proxy = new SqlProxy
      val conn = DbUtil.getConnection
      try {
        //需求二:计算商品课程页面到订单页的跳转转换率、订单页面到支付页面的跳转转换率
        calConvertRate(proxy, conn)
        val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (elem <- ranges) {
          val sql_update_offset = "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?) "
          proxy.executeUpdate(conn, sql_update_offset, Array(
            groupid, topics(0), elem.partition, elem.untilOffset
          ))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        proxy.shutdown(conn)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * @author lyf3312
    * @date 20/04/11 20:38
    * @description 计算页面点击数及跳转率
    *
    */
  def calPageCount(proxy: SqlProxy, conn: Connection, item: (String, Int)): Unit = {
    //解析各页面pv
    var clickNum = item._2.toLong
    val keys = item._1.split("_")
    val last_page_id = keys(0).toInt
    val page_id = keys(1).toInt
    val next_page_id = keys(2).toInt
    //写入各页面浏览数
    //1.获取历史点击
    proxy.executeQuery(conn, "select num from `page_jump_rate` where page_id=? ", Array(page_id), new QueryCallback {
      override def process(resultSet: ResultSet): Unit = {
        while (resultSet.next()) {
          clickNum += resultSet.getLong("num")
        }
        resultSet.close()
      }
    }
    )
    //2.更新num
    if (page_id == 1) {
      //首页,简单处理,跳转率设置为1
      proxy.executeUpdate(conn, "insert into page_jump_rate(last_page_id,page_id,next_page_id,num,jump_rate) " +
        "values(?,?,?,?,?) on duplicate key update num=?", Array(last_page_id, page_id, next_page_id, clickNum, "100%", clickNum))
    } else {
      //转换率,单独计算
      proxy.executeUpdate(conn, "insert into page_jump_rate(last_page_id,page_id,next_page_id,num) " +
        "values(?,?,?,?) on duplicate key update num=?", Array(last_page_id, page_id, next_page_id, clickNum, clickNum))
    }
  }

  /**
    * @author lyf3312
    * @date 20/04/11 20:56
    * @description 计算页面跳转转换率
    *
    */
  def calConvertRate(proxy: SqlProxy, conn: Connection) = {
    //初始化个页面访问量
    var page1_num = 0l
    var page2_num = 0l
    var page3_num = 0l
    //从MySQL中获取各页面访问量
    proxy.executeQuery(conn, "select num from page_jump_rate where page_id=? ", Array(1), new QueryCallback {
      override def process(resultSet: ResultSet): Unit = {
        while (resultSet.next()) {
          page1_num = resultSet.getLong(1)
        }
      }
    })
    proxy.executeQuery(conn, "select num from page_jump_rate where page_id=? ", Array(2), new QueryCallback {
      override def process(resultSet: ResultSet): Unit = {
        while (resultSet.next()) {
          page2_num = resultSet.getLong(1)
        }
      }
    })
    proxy.executeQuery(conn, "select num from page_jump_rate where page_id=? ", Array(3), new QueryCallback {
      override def process(resultSet: ResultSet): Unit = {
        while (resultSet.next()) {
          page3_num = resultSet.getLong(1)
        }
      }
    })

    //计算跳转率
    val nf = NumberFormat.getPercentInstance
    val page1ToPage2 = if (page1_num == 0) "0%" else nf.format(page2_num.toDouble / page1_num.toDouble)
    val page2ToPage3 = if (page2_num == 0) "0%" else nf.format(page3_num.toDouble / page2_num.toDouble)

    //写入到mysql
    proxy.executeUpdate(conn, "update page_jump_rate set jump_rate=? where page_id=?", Array(page1ToPage2, 2))
    proxy.executeUpdate(conn, "update page_jump_rate set jump_rate=? where page_id=?", Array(page2ToPage3, 3))
  }

}
