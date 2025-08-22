package com.retailersv1.stream;

import com.stream.common.utils.ConfigUtils;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

import java.util.Properties;

/**
 * @Package com.stream.utils
 * @Author xiaoye
 * @Date 2025/8/21 14:41
 * @description:
 */
public class DorisUtils {
    private static final String DORIS_FE_NODES = ConfigUtils.getString("doris.fe.nodes");
    private static final String DORIS_DATABASE = ConfigUtils.getString("doris.database");

    public static DorisSink<String> getDorisSink(String table) {
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据
        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes(DORIS_FE_NODES)
                        .setTableIdentifier(DORIS_DATABASE + "." + table)
                        .setUsername("root")
                        .setPassword("123456")
                        .build()
                )
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
//                        .setLabelPrefix(labelPrefix)  // stream-load 导入数据时 label 的前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setBufferCount(3) // 批次条数: 默认 3
                        .setBufferSize(1024 * 1024) // 批次大小: 默认 1M
                        .setCheckInterval(3000) // 批次输出间隔  上述三个批次的限制条件是或的关系
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
        return sink;
    }
}
