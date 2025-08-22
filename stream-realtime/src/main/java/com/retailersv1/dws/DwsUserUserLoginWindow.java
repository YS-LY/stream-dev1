package com.retailersv1.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.retailersv1.domain.UserLoginBean;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.DateTimeUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

public class DwsUserUserLoginWindow {
    // Kafka配置
    private static final String kafka_bootstrap_server = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_page_topic = ConfigUtils.getString("kafka.page.topic");

    // MySQL配置（从配置文件读取或硬编码）
    private static final String MYSQL_URL = "jdbc:mysql://cdh01:3306/gmall2025?useSSL=false&serverTimezone=Asia/Shanghai";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "123456";

    @SneakyThrows
    public static void main(String[] args) {
        // 1. 初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setStateBackend(new MemoryStateBackend());

        // 2. 从Kafka读取数据
        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_bootstrap_server,
                        kafka_page_topic,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ), WatermarkStrategy.noWatermarks(), "read_kafka_page_topic"
        );

        // 3. 转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSourceDs.map(JSON::parseObject);

        // 4. 过滤登录行为
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        // 确保有uid，且来源是登录页或直接登录（last_page_id为空）
                        String uid = jsonObj.getJSONObject("common").getString("uid");
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        return StringUtils.isNotEmpty(uid) &&
                                ("login".equals(lastPageId) || StringUtils.isEmpty(lastPageId));
                    }
                }
        );

        // 5. 指定水印（事件时间）
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts"); // 使用事件时间戳
                                    }
                                }
                        )
        );

        // 6. 按uid分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("uid")
        );

        // 7. 状态编程：计算独立用户(uu)和回流用户(back)
        SingleOutputStreamOperator<UserLoginBean> beanDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
                    private ValueState<String> lastLoginDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化状态：存储用户上次登录日期
                        ValueStateDescriptor<String> descriptor =
                                new ValueStateDescriptor<>("lastLoginDateState", String.class);
                        lastLoginDateState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<UserLoginBean> out) throws Exception {
                        Long ts = jsonObj.getLong("ts");
                        String curLoginDate = DateTimeUtils.tsToDate(ts); // 当天日期（yyyy-MM-dd）
                        String lastLoginDate = lastLoginDateState.value(); // 上次登录日期

                        Long uuCt = 0L; // 独立用户数标记
                        Long backCt = 0L; // 回流用户数标记

                        if (StringUtils.isNotEmpty(lastLoginDate)) {
                            // 有历史登录记录
                            if (!lastLoginDate.equals(curLoginDate)) {
                                // 非当天首次登录：记为独立用户
                                uuCt = 1L;
                                lastLoginDateState.update(curLoginDate); // 更新登录日期

                                // 计算间隔天数，>=8天则记为回流用户
                                long dayDiff = (ts - DateTimeUtils.dateToTs(lastLoginDate))
                                        / (1000 * 60 * 60 * 24);
                                if (dayDiff >= 8) {
                                    backCt = 1L;
                                }
                            }
                        } else {
                            // 无历史记录：首次登录，记为独立用户
                            uuCt = 1L;
                            lastLoginDateState.update(curLoginDate);
                        }

                        // 输出有效指标（至少有一个指标不为0）
                        if (uuCt != 0 || backCt != 0) {
                            out.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                        }
                    }
                }
        );

        // 8. 开窗（1秒滚动窗口，可根据需求调整）
        AllWindowedStream<UserLoginBean, TimeWindow> windowDS = beanDS.windowAll(
                TumblingProcessingTimeWindows.of(Time.seconds(1))
        );

        // 9. 窗口聚合：汇总指标
        SingleOutputStreamOperator<UserLoginBean> reduceDS = windowDS.reduce(
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) {
                        // 累加独立用户数和回流用户数
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                },
                new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) {
                        UserLoginBean bean = values.iterator().next();
                        // 补充窗口时间信息
                        bean.setStt(DateTimeUtils.tsToDateTime(window.getStart()));
                        bean.setEdt(DateTimeUtils.tsToDateTime(window.getEnd()));
                        bean.setCurDate(DateTimeUtils.tsToDate(window.getStart()));
                        out.collect(bean);
                    }
                }
        );

        // 10. 写入MySQL
        reduceDS.addSink(
                JdbcSink.sink(
                        // SQL：插入或更新（主键冲突时更新）
                        "INSERT INTO dws_user_user_login_window " +
                                "(stt, edt, cur_date, back_ct, uu_ct, ts) " +
                                "VALUES (?, ?, ?, ?, ?, ?) " +
                                "ON DUPLICATE KEY UPDATE " +
                                "back_ct = VALUES(back_ct), " +
                                "uu_ct = VALUES(uu_ct), " +
                                "ts = VALUES(ts)",

                        // 字段映射：将UserLoginBean转换为SQL参数
                        (statement, bean) -> {
                            statement.setString(1, bean.getStt());       // 窗口起始时间
                            statement.setString(2, bean.getEdt());       // 窗口结束时间
                            statement.setString(3, bean.getCurDate());   // 统计日期
                            statement.setLong(4, bean.getBackCt());      // 回流用户数
                            statement.setLong(5, bean.getUuCt());        // 独立用户数
                            statement.setLong(6, bean.getTs());          // 时间戳
                        },

                        // 执行配置：批量提交
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1)          // 测试用：1条提交
                                .withBatchIntervalMs(1000) // 1秒批量提交
                                .withMaxRetries(3)         // 失败重试3次
                                .build(),

                        // 连接配置
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(MYSQL_URL)
                                .withUsername(MYSQL_USER)
                                .withPassword(MYSQL_PASSWORD)
                                .withDriverName("com.mysql.cj.jdbc.Driver") // MySQL 8.0+驱动
                                .build()
                )
        ).name("MySQL-Login-Sink");

        // 启动任务
        env.execute("DwsUserUserLoginWindow-To-MySQL");
    }
}