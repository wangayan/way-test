package com.retailersv.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;


/**
 * DWS 层：统一“流量/行为”明细打平汇总到一个主题
 * 1) 读取 DWD 层的 start/page/display/action 四类日志
 * 2) 统一补齐字段：event_type、mid、event_ts
 * 3) 合并成一条统一流，写出到 Kafka 主题 dws_user_action_detail
 */
public class DwsTrafficBehaviorSummary {

    public static void main(String[] args) throws Exception {
        // 1. 环境准备：获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4); // 并行度（按集群资源调整）
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-dws"); // 本地测试 checkpoint 目录

        // 2. 从 Kafka 读取各类 DWD 流（均为 JSON 字符串）
        String kafkaServer = "cdh01:9092"; // Kafka broker 地址

        // 2.1 启动日志流：topic = dwd_start_log
        DataStream<String> startStream = env.fromSource(
                KafkaUtils.getKafkaSource("dwd_start_log", "dwd_start_log", kafkaServer), // 自定义工具类创建 Source
                WatermarkStrategy.noWatermarks(),   // 无水位线（后续若要按事件时间窗口，需要定义 WatermarkStrategy）
                "dwd_start_log"                     // Source 名称
        );

        // 2.2 页面日志流：topic = dwd_traffic_page_log
        DataStream<String> pageStream = env.fromSource(
                KafkaUtils.getKafkaSource("dwd_traffic_page_log", "dwd_traffic_page_log", kafkaServer),
                WatermarkStrategy.noWatermarks(),
                "dwd_traffic_page_log"
        );

        // 2.3 曝光日志流：topic = dwd_traffic_display_log
        DataStream<String> displayStream = env.fromSource(
                KafkaUtils.getKafkaSource("dwd_traffic_display_log", "dwd_traffic_display_log", kafkaServer),
                WatermarkStrategy.noWatermarks(),
                "dwd_traffic_display_log"
        );

        // 2.4 行为日志流：topic = dwd_action_log
        DataStream<String> actionStream = env.fromSource(
                KafkaUtils.getKafkaSource("dwd_action_log", "dwd_action_log", kafkaServer),
                WatermarkStrategy.noWatermarks(),
                "dwd_action_log"
        );

        // 3. 统一结构：补充 event_type、mid、event_ts 字段，并合并成一条流
        DataStream<JSONObject> unifiedStream = startStream
                // 3.1 start：从 JSON 根级读取 ts，并从 common.mid 取设备ID
                .map(JSON::parseObject) // String -> JSONObject
                .map(json -> {
                    json.put("event_type", "start");                                  // 事件类型
                    json.put("mid", json.getJSONObject("common").getString("mid"));   // 设备ID
                    json.put("event_ts", json.getLong("ts"));                          // 事件时间戳（毫秒）
                    return json;
                })
                // 3.2 合并 page 流：page 同样从根级 ts、common.mid
                .union(
                        pageStream.map(JSON::parseObject).map(json -> {
                            json.put("event_type", "page");
                            json.put("mid", json.getJSONObject("common").getString("mid"));
                            json.put("event_ts", json.getLong("ts"));
                            return json;
                        }),
                        // 3.3 合并 display 流：display 通常有 displays 数组，但此处仅打标签与通用字段
                        displayStream.map(JSON::parseObject).map(json -> {
                            json.put("event_type", "display");
                            json.put("mid", json.getJSONObject("common").getString("mid"));
                            json.put("event_ts", json.getLong("ts"));
                            return json;
                        }),
                        // 3.4 合并 action 流：注意大多数埋点结构为 actions 数组
                        // 这里代码假设存在单个 "action" 对象，若为 "actions" 数组，需在此处 flatMap 展开
                        actionStream.map(JSON::parseObject).map(json -> {
                            json.put("event_type", "action");
                            // 取通用字段 common.mid
                            JSONObject common = json.getJSONObject("common");
                            // ⚠️ 注意：常见结构是 "actions": [ {action_id, item, ts, ...}, ... ]
                            // 你当前代码使用的是 "action" 单对象；如果实际为数组需要改为遍历。
                            JSONObject action = json.getJSONObject("action");
                            json.put("mid", common.getString("mid"));
                            // 行为时间戳通常在 action(s) 内部
                            json.put("event_ts", action.getLongValue("ts"));
                            return json;
                        })
                );

        // 4. 控制台打印调试（上线可移除或降采样）
        unifiedStream.print();

        // 5. Kafka Sink 构建：写出到 DWS 主题 dws_user_action_detail
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092") // Kafka broker
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("dws_user_action_detail")          // 目标主题
                                .setValueSerializationSchema(new SimpleStringSchema()) // value 序列化
                                .build()
                )
                .setKafkaProducerConfig(new Properties() {{ // 生产者额外参数
                    put(ProducerConfig.ACKS_CONFIG, "all"); // 最强一致性，等待所有副本确认
                }})
                .build();

        // 6. 序列化为字符串写出到 Kafka
        unifiedStream.map(JSON::toJSONString).sinkTo(kafkaSink);

        // 7. 启动作业
        env.execute("DWS Traffic Behavior Summary");
    }
}
