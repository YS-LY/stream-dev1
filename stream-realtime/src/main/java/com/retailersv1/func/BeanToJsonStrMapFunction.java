package com.retailersv1.func;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.api.common.functions.MapFunction;

public class BeanToJsonStrMapFunction<T> implements MapFunction<T, String> {

    // 初始化Jackson的ObjectMapper，作为全局实例复用
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        // 配置全局的命名策略为蛇形命名
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }

    @Override
    public String map(T bean) throws Exception {
        try {
            // 将对象序列化为JSON字符串
            return objectMapper.writeValueAsString(bean);
        } catch (JsonProcessingException e) {
            // 处理序列化异常
            throw new Exception("JSON序列化失败: " + e.getMessage(), e);
        }
    }
}
