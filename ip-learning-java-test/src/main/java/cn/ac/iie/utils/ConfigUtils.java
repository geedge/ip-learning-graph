package cn.ac.iie.utils;

import java.util.Properties;

public class ConfigUtils {
    private static Properties propCommon = new Properties();

    public static String getStringProperty(String key) {
            return propCommon.getProperty(key);
    }


    public static Integer getIntProperty(String key) {
            return Integer.parseInt(propCommon.getProperty(key));
    }

    public static Long getLongProperty(String key) {
            return Long.parseLong(propCommon.getProperty(key));
    }

    public static Boolean getBooleanProperty(String key) {
            return "true".equals(propCommon.getProperty(key).toLowerCase().trim());
    }

    static {
        try {
            propCommon.load(ConfigUtils.class.getClassLoader().getResourceAsStream("application.properties"));
            System.out.println("application.properties加载成功");


        } catch (Exception e) {
            propCommon = null;
            System.err.println("配置加载失败");
        }
    }
}
