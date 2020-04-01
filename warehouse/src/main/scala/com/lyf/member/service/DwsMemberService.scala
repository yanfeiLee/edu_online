package com.lyf.member.service

import com.lyf.common.Constant
import com.lyf.member.beans.{MemberZipper, MemberZipperResult}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/01 19:09
  * Version: 1.0
  */
object DwsMemberService {

  /**
    * @author lyf3312
    * @date 20/04/01 19:12
    * @description 利用sql实现用户宽表合成及用户支付金额、用户等级拉链表
    *
    */
  def importMember(ss: SparkSession, time: String): Unit = {
     import ss.implicits._
    //生成用户大宽表
    ss.sql(
      s"""select
         |    uid,first(ad_id),first(fullname),first(iconurl),first(lastlogin),
         |    first(mailaddr),first(memberlevel),first(password),sum(cast(paymoney as decimal(10,4))),first(phone),first(qq),
         |    first(register),first(regupdatetime),first(unitname),first(userip),first(zipcode),
         |    first(appkey),first(appregurl),first(bdp_uuid),first(reg_createtime),first(domain),
         |    first(isranreg),first(regsource),first(regsourcename),first(adname),first(siteid),first(sitename),
         |    first(siteurl),first(site_delete),first(site_createtime),first(site_creator),first(vip_id),max(vip_level),
         |    min(vip_start_time),max(vip_end_time),max(vip_last_modify_time),first(vip_max_free),first(vip_min_free),max(vip_next_level),
         |    first(vip_operator),dt,dn
         |from
         |       (
         |            select
         |                a.uid,a.ad_id,a.fullname,a.iconurl,a.lastlogin,a.mailaddr,a.memberlevel,
         |                a.password,e.paymoney,a.phone,a.qq,a.register,a.regupdatetime,a.unitname,a.userip,
         |                a.zipcode,a.dt,b.appkey,b.appregurl,b.bdp_uuid,b.createtime as reg_createtime,b.domain,b.isranreg,b.regsource,
         |                b.regsourcename,c.adname,d.siteid,d.sitename,d.siteurl,d.delete as site_delete,d.createtime as site_createtime,
         |                d.creator as site_creator,f.vip_id,f.vip_level,f.start_time as vip_start_time,f.end_time as vip_end_time,
         |                f.last_modify_time as vip_last_modify_time,f.max_free as vip_max_free,f.min_free as vip_min_free,
         |                f.next_level as vip_next_level,f.operator as vip_operator,a.dn
         |            from
         |                dwd.dwd_member a left join dwd.dwd_member_regtype b on a.uid=b.uid  and a.dn=b.dn
         |                left join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn
         |                left join dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn
         |                left join dwd.dwd_pcentermempaymoney e on a.uid=e.uid and a.dn=e.dn
         |                left join dwd.dwd_vip_level f on e.vip_id=f.vip_id and e.dn=f.dn
         |            where a.dt='${time}'
         |        )
         |group by uid,dn,dt""".stripMargin)
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto(Constant.DWS_TB_MEMBER)

    //生成用户支付及vip等级拉链表
    //查询拉链表历史数据
    val historyMemberZipper = ss.sql("select * from " + Constant.DWS_TB_MEMBER_ZIPPER).as[MemberZipper]
    //查询当日新增数据
    val dayMemberZipper = ss.sql(
      s"""
         |select
         |    a.uid,
         |    sum(cast(a.paymoney as decimal(10,4))) as paymoney,
         |    max(b.vip_level) as vip_level,
         |    from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd') as start_time,
         |    '9999-12-31' as end_time,
         |    first(a.dn) as dn
         |from
         |    dwd.dwd_pcentermempaymoney a
         |    join dwd.dwd_vip_level b on a.vip_id=b.vip_id and a.dn=b.dn
         |where a.dt='$time'
         |group by uid""".stripMargin).as[MemberZipper]
    val unit = historyMemberZipper.union(dayMemberZipper)
      .groupByKey(item => item.uid + "_" + item.dn + item) // (100_webA,(memberZip,))
      .mapGroups {
      case (key, iters) => {
        val zippers = iters.toList.sortBy(_.start_time)
        //购买记录大于1，而且最后今日有购买记录，则更新上一此购买后vip的end_time
        if (zippers.size > 1 && zippers.last.start_time == time) {
          val oldLastRecord = zippers(zippers.size - 2)
          val lastRecord = zippers.last
          oldLastRecord.end_time = lastRecord.start_time //更新倒数第二条记录的end_time
          lastRecord.paymoney = (BigDecimal.apply(oldLastRecord.paymoney) + BigDecimal.apply(lastRecord.paymoney)).toString()
        }
        MemberZipperResult(zippers)
      }
    }.flatMap(_.list)
      .coalesce(3).write.mode(SaveMode.Overwrite).insertInto(Constant.DWS_TB_MEMBER_ZIPPER)

  }
}
