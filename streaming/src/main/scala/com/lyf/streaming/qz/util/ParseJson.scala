package com.lyf.streaming.qz.util

import com.alibaba.fastjson.{JSON, JSONObject}



/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/10 10:25
  * Version: 1.0
  */
object ParseJson {
  def getJsonObj(json:String): JSONObject ={
    var res:JSONObject = null
    try{
      res = JSON.parseObject(json)
    }catch {
      case e: Exception =>
        println("Json 字符串格式化异常")
    }
    res
  }
}
