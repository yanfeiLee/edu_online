package com.lyf.member.beans

/**
  * Project: edu_online
  * Create by lyf3312 on 20/04/01 19:34
  * Version: 1.0
  */
case class MemberZipper(
                         uid: Int,
                         var paymoney: String,
                         vip_level: String,
                         start_time: String,
                         var end_time: String,
                         dn: String
                       )

case class MemberZipperResult(list: List[MemberZipper])


case class QueryResult(
                        uid: Int,
                        ad_id: Int,
                        memberlevel: String,
                        register: String,
                        appregurl: String, //注册来源url
                        regsource: String,
                        regsourcename: String,
                        adname: String,
                        siteid: String,
                        sitename: String,
                        vip_level: String,
                        paymoney: BigDecimal,
                        dt: String,
                        dn: String
                      )

case class DwsMember(
                      uid: Int,
                      ad_id: Int,
                      fullname: String,
                      iconurl: String,
                      lastlogin: String,
                      mailaddr: String,
                      memberlevel: String,
                      password: String,
                      paymoney: BigDecimal,
                      phone: String,
                      qq: String,
                      register: String,
                      regupdatetime: String,
                      unitname: String,
                      userip: String,
                      zipcode: String,
                      appkey: String,
                      appregurl: String,
                      bdp_uuid: String,
                      reg_createtime: String,
                      domain: String,
                      isranreg: String,
                      regsource: String,
                      regsourcename: String,
                      adname: String,
                      siteid: String,
                      sitename: String,
                      siteurl: String,
                      site_delete: String,
                      site_createtime: String,
                      site_creator: String,
                      vip_id: String,
                      vip_level: String,
                      vip_start_time: String,
                      vip_end_time: String,
                      vip_last_modify_time: String,
                      vip_max_free: String,
                      vip_min_free: String,
                      vip_next_level: String,
                      vip_operator: String,
                      dt: String,
                      dn: String
                    )


case class DwsMember_Result(
                             uid: Int,
                             ad_id: Int,
                             fullname: String,
                             icounurl: String,
                             lastlogin: String,
                             mailaddr: String,
                             memberlevel: String,
                             password: String,
                             paymoney: String,
                             phone: String,
                             qq: String,
                             register: String,
                             regupdatetime: String,
                             unitname: String,
                             userip: String,
                             zipcode: String,
                             appkey: String,
                             appregurl: String,
                             bdp_uuid: String,
                             reg_createtime: String,
                             domain: String,
                             isranreg: String,
                             regsource: String,
                             regsourcename: String,
                             adname: String,
                             siteid: String,
                             sitename: String,
                             siteurl: String,
                             site_delete: String,
                             site_createtime: String,
                             site_creator: String,
                             vip_id: String,
                             vip_level: String,
                             vip_start_time: String,
                             vip_end_time: String,
                             vip_last_modify_time: String,
                             vip_max_free: String,
                             vip_min_free: String,
                             vip_next_level: String,
                             vip_operator: String,
                             dt: String,
                             dn: String
                           )

