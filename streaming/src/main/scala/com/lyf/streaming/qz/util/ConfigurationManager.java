package com.lyf.streaming.qz.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Project: edu_online
 * Create by lyf3312 on 20/04/07 18:27
 * Version: 1.0
 */
public class ConfigurationManager {
    private static Properties prop = new Properties();

    //类加载时初始化prop
    static{
        try {
            InputStream is = ConfigurationManager.class.getClassLoader().getResourceAsStream("dbconfig.properties");
            prop.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //获取String配置信息
    public static String getProperty(String key){
        return  prop.getProperty(key);
    }

    //获取Boolean类型配置信息
    public static boolean getBooleanProperties(String key){
        String value = prop.getProperty(key);
        try{
            return Boolean.valueOf(value);
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
}
