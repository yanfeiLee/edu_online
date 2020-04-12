package com.lyf.streaming.qz.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * Project: edu_online
 * Create by lyf3312 on 20/03/31 22:42
 * Version: 1.0
 */
public class ParseJsonData {
    public static JSONObject getJsonObject(String jsonData){
        try {
            return JSON.parseObject(jsonData);
        }catch (Exception e){
            return null;
        }
    }
}
