package com.lyf.common;

/**
 * Project: edu_online
 * Create by lyf3312 on 20/03/31 22:21
 * Version: 1.0
 */
public class Constant {
    //member模块配置常量

    //ods层常量
    private static final String ODS_BASE_PATH = "/user/lee/ods/";
    //log path

    public static final String ODS_LOG_BASEADLOG = ODS_BASE_PATH + "baseadlog.log";
    public static final String ODS_LOG_MEMBER = ODS_BASE_PATH + "member.log";
    public static final String ODS_LOG_BASEWEBSIT = ODS_BASE_PATH + "baswewebsite.log";
    public static final String ODS_LOG_MEMBERREGTYPE = ODS_BASE_PATH + "memberRegtype.log";
    public static final String ODS_LOG_PCENTERMEMPAYMONEY = ODS_BASE_PATH + "pcentermempaymoney.log";
    public static final String ODS_LOG_PCENTERMEMVIPLEVEL = ODS_BASE_PATH + "pcenterMemViplevel.log";

    //dwd层常量
    //dwd table name
    public static final String DWD_TB_BASE_AD = "dwd.dwd_base_ad";
    public static final String DWD_TB_BASE_WEBSITE = "dwd.dwd_base_website";
    public static final String DWD_TB_MEMBER = "dwd.dwd_member";
    public static final String DWD_TB_MEMBER_REG_TYPE = "dwd.dwd_member_regtype";
    public static final String DWD_TB_PCENTER_MEM_PAYMONEY = "dwd.dwd_pcentermempaymoney";
    public static final String DWD_TB_VIP_LEVEL = "dwd.dwd_vip_level";

    //dws层常量
    public static final String DWS_TB_MEMBER = "dws.dws_member";
    public static final String DWS_TB_MEMBER_ZIPPER = "dws.dws_member_zipper";


    //ads层常量
    public static final String ADS_TB_REGISTER_APPREGURLNUM = "ads.ads_register_appregurlnum";
    public static final String ADS_TB_REGISTER_SITENAMENUM = "ads.ads_register_sitenamenum";
    public static final String ADS_TB_REGISTER_REGSOURCENAMENUM = "ads.ads_register_regsourcenamenum";
    public static final String ADS_TB_REGISTER_ADNAMENUM = "ads.ads_register_adnamenum";
    public static final String ADS_TB_REGISTER_MEMBERLEVELNUM = "ads.ads_register_memberlevelnum";
    public static final String ADS_TB_REGISTER_VIPLEVELNUM = "ads.ads_register_viplevelnum";
    public static final String ADS_TB_REGISTER_TOP3MEMBERPAY = "ads.ads_register_top3memberpay";

}
