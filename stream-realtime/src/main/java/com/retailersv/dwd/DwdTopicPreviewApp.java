package com.retailersv.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 功能：快速预览 DWD 层 Kafka 主题的数据
 * 特点：
 *  - 每个主题只打印一条消息，避免刷屏
 *  - 支持多个 DWD 主题（交易、用户、互动、工具等）
 */
public class DwdTopicPreviewApp {
    // 从配置文件读取 Kafka 集群地址
    private static final String KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");

    public static void main(String[] args) throws Exception {
        // 设置 Hadoop 用户，避免权限问题
        System.setProperty("HADOOP_USER_NAME", "root");

        // 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env); // 应用统一参数配置
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-DWS"); // 设置 checkpoint 存储目录

        // 定义需要预览的 Kafka 主题
        String[] topics = {
                "dwd_trade_order_detail",   // 订单明细
                "dwd_trade_pay_detail",     // 支付明细
                "dwd_trade_order_refund",   // 订单退款
                "dwd_user_register",        // 用户注册
                "dwd_interaction_favor_add",// 点赞/收藏行为
                "dwd_tool_coupon_get",      // 领券
                "dwd_dynamic_fallback"      // 兜底日志
        };

        // 为每个主题创建数据流，预览一条消息
        for (String topic : topics) {
            previewOneMessage(env, topic);
        }

        // 启动作业
        env.execute("DwdTopicPreviewApp");
    }

    /**
     * 预览指定 Kafka 主题的一条消息
     */
    private static void previewOneMessage(StreamExecutionEnvironment env, String topic) {
        // 从 Kafka 读取数据，起始 offset 为 earliest（最早消息）
        DataStreamSource<String> source = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_SERVER, topic, "preview_group_" + topic, OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(), // 不使用 watermark
                topic + "_source"
        );

        // 每个主题只打印一条消息
        source.flatMap(new OneMessagePrinter(topic));
    }

    /**
     * 自定义 FlatMap，只打印每个主题的一条消息
     */
    static class OneMessagePrinter extends RichFlatMapFunction<String, String> {
        // 静态 map：记录每个 topic 是否已经打印过
        private static final Map<String, Boolean> printed = new HashMap<>();
        private final String topic;

        public OneMessagePrinter(String topic) {
            this.topic = topic;
        }

        @Override
        public void open(Configuration parameters) {
            // 初始化状态（如果没有，则标记为 false）
            printed.putIfAbsent(topic, false);
        }

        @Override
        public void flatMap(String value, Collector<String> out) {
            // 如果该 topic 还没有打印过，则输出一条消息，并标记为 true
            if (!printed.get(topic)) {
                out.collect(topic + " -> " + value); // 控制台打印：topic -> 消息内容
                printed.put(topic, true);
            }
        }
    }
}
