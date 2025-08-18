package com.stream.common.utils;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * 配置文件工具类
 * time: 2021/8/11 9:48 className: ConfigUtils.java
 *
 * @author han.zhou
 * @version 1.0.0
 */
public final class ConfigUtils {
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

    private static Properties properties;

    static {
        properties = new Properties();
        try {
            properties.load(ConfigUtils.class.getClassLoader().getResourceAsStream("common-config.properties"));
        } catch (IOException e) {
            logger.error("加载配置文件出错, exit 1", e);
            System.exit(1);
        }
    }

    public static String getString(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            logger.error("配置项未找到: {}", key);
            throw new RuntimeException("配置项未找到: " + key);
        }
        return value.trim();
    }

    public static int getInt(String key) {
        String value = getString(key);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            logger.error("配置项格式错误: {} = {}", key, value);
            throw new RuntimeException("配置项格式错误: " + key + " = " + value, e);
        }
    }

    public static int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (Strings.isNullOrEmpty(value)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            logger.error("配置项格式错误: {} = {}", key, value);
            throw new RuntimeException("配置项格式错误: " + key + " = " + value, e);
        }
    }

    public static long getLong(String key) {
        String value = getString(key);
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            logger.error("配置项格式错误: {} = {}", key, value);
            throw new RuntimeException("配置项格式错误: " + key + " = " + value, e);
        }
    }

    public static long getLong(String key, long defaultValue) {
        String value = properties.getProperty(key);
        if (Strings.isNullOrEmpty(value)) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            logger.error("配置项格式错误: {} = {}", key, value);
            throw new RuntimeException("配置项格式错误: " + key + " = " + value, e);
        }
    }
}