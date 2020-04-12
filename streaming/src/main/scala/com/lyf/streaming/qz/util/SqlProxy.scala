package com.lyf.streaming.qz.util

import java.sql.{Connection, PreparedStatement, ResultSet}

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/07 18:53
  * Version: 1.0
  * sql代理,发送到MySQL 执行SQL语句
  */
trait QueryCallback{
  def process(resultSet:ResultSet)
}
class SqlProxy {
  private var rs:ResultSet = _
  private var psmt:PreparedStatement = _

  //查询功能
  def executeQuery(conn: Connection, sql: String, params: Array[Any], queryCallback: QueryCallback): Unit ={
    rs = null
    try {
      psmt = conn.prepareStatement(sql)
      if(params !=null && params.length>0){
        for (i <- 0 until params.length) {
          psmt.setObject(i+1,params(i))
        }
      }
      rs = psmt.executeQuery()
      queryCallback.process(rs)
    }catch {
      case e:Exception => e.printStackTrace()
    }

  }
  //更新功能及插入功能
  def executeUpdate(conn: Connection, sql: String, params: Array[Any]): Int = {
    var rtn  = 0
    try{
      psmt = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          psmt.setObject(i + 1, params(i))
        }
      }
      rtn = psmt.executeUpdate()
    }catch {
      case e:Exception => e.printStackTrace()
    }
    rtn
  }
  //关闭连接
  def shutdown(conn:Connection): Unit ={
    DbUtil.closeResource(rs,psmt,conn)
  }

}
