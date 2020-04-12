package com.lyf.streaming.qz.producer

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.lyf.streaming.qz.util.{ParseJson, ParseJsonData}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/10 11:56
  * Version: 1.0
  */
object PageProducer {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark job").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    sc.textFile("file://"+this.getClass.getResource("/page.log"),10)
      .foreachPartition(partition => {
        //kafka参数
        val props = new Properties()
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
        props.put("acks", "1")
        props.put("batch.size", "16384")
        props.put("linger.ms", "10")
        props.put("buffer.memory", "33554432")
        props.put("key.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        //每个分区创建一个producer
        val producer = new KafkaProducer[String, String](props)
        //遍历分区内所有数据并将数据发送出去
        partition.foreach(item => {
          producer.send(new ProducerRecord[String, String]("page_topic", item))
        })
        producer.flush()
        producer.close()
      })

    sc.stop()
  }
}
