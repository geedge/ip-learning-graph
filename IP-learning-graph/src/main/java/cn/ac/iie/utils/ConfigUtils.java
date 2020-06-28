package cn.ac.iie.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ConfigUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigUtils.class);
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
            LOG.info("application.properties加载成功");


        } catch (Exception e) {
            propCommon = null;
            LOG.error("配置加载失败");
        }
    }
}
