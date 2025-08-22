package com.retailersv;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DbusBusinessCdc2DwdKafka {

    private static final String KAFKA_SERVERS = "cdh01:9092,cdh02:9092,cdh03:9092";
    private static final String ODS_TOPIC = "ods_ecommerce_order";
    private static final String DEFAULT_DWD_TOPIC = "dwd_dynamic_fallback";

    // 定义你需要的表 + 映射关系
    private static final Map<String, String> TABLE_SINK_MAP = new HashMap<>();
    static {
        TABLE_SINK_MAP.put("favor_info_insert", "dwd_interaction_favor_add");
        TABLE_SINK_MAP.put("coupon_use_insert", "dwd_tool_coupon_get");
        TABLE_SINK_MAP.put("coupon_use_update", "dwd_tool_coupon_use");
        TABLE_SINK_MAP.put("order_info_insert", "dwd_trade_order_detail");
        TABLE_SINK_MAP.put("order_info_update", "dwd_trade_order_pay");
        TABLE_SINK_MAP.put("refund_payment_insert", "dwd_trade_order_refund");
        TABLE_SINK_MAP.put("payment_info_insert", "dwd_trade_pay_detail");
        TABLE_SINK_MAP.put("user_info_insert", "dwd_user_register");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000L);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-chk");

        // 1) 读取 ODS 主题
        KafkaSource<String> odsSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_SERVERS)
                .setTopics(ODS_TOPIC)
                .setGroupId("flink_dwd_business_group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> odsStream = env.fromSource(
                odsSource, WatermarkStrategy.noWatermarks(), "Kafka ODS Source");

        // 2) 解析 JSON & 过滤业务表
        SingleOutputStreamOperator<String> parsedStream = odsStream.map(json -> {
            try {
                JSONObject obj = JSONObject.parseObject(json);
                String table = obj.getJSONObject("source").getString("table");
                String op = obj.getString("op");  // c/u/d
                JSONObject after = obj.getJSONObject("after");

                if (after == null) return null;

                String sourceType = null;
                // snapshot("r") 也当 insert
                if ("c".equals(op) || "r".equals(op)) sourceType = "insert";
                else if ("u".equals(op)) sourceType = "update";
                else if ("d".equals(op)) sourceType = "delete";


                String key = table + "_" + sourceType;
                String sinkTable = TABLE_SINK_MAP.getOrDefault(key, DEFAULT_DWD_TOPIC);

                JSONObject result = new JSONObject();
                result.put("sink_table", sinkTable);
                result.put("data", after);

                return result.toJSONString();
            } catch (Exception e) {
                System.err.println("[WARN] parse error: " + json);
                return null;
            }
        }).filter(Objects::nonNull);

        parsedStream.print(">>> DWD out");

        // 3) 按 sink_table 动态写回 Kafka
        KafkaSink<String> dynamicSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopicSelector(value -> {
                                    try {
                                        JSONObject obj = JSONObject.parseObject((String) value);
                                        return obj.getString("sink_table");
                                    } catch (Exception e) {
                                        return DEFAULT_DWD_TOPIC;
                                    }
                                })
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                ).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        parsedStream.sinkTo(dynamicSink).name("Kafka Dynamic Sink");

        env.execute("ODS to DWD Business Kafka");
    }
}
