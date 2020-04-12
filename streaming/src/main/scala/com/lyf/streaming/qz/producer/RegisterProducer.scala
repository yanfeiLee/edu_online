package com.lyf.streaming.qz.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/07 19:15
  * Version: 1.0
  */
object RegisterProducer {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark job").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //读取日志文件
    sc.textFile("file://"+this.getClass.getResource("/register.log"),10)
        .foreachPartition(rdd=>{
          //以分区为单位,将数据写入kafka
          //设置连接kafka参数
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

          val producer = new KafkaProducer[String,String](props)
          //发送数据
          rdd.foreach(item=>{
            producer.send(new ProducerRecord[String,String]("register_topic",item))
          })
          producer.flush()
          producer.close()
        })


    sc.stop()
  }
}
