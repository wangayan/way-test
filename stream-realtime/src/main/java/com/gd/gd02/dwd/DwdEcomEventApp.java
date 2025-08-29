package com.gd.gd02.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * @Title: DwdEcomEventApp
 * @Author wang.Ayan
 * @Date 2025/8/28
 * @Package com.gd.gd02.dwd
 * @description: DWD层：清洗、拉宽 ODS 层数据，构建店铺绩效宽表
 * 工单编号：大数据-用户画像-02-服务主题店铺绩效
 */
public class DwdEcomEventApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);

        String kafkaBootstrap = "cdh01:9092,cdh02:9092,cdh03:9092";

        // 消费多个 ODS 层 topic
        KafkaSource<String> consultLogSource = createKafkaSource(kafkaBootstrap, "ods_consult_log", "dwd_consult_log");
        KafkaSource<String> orderInfoSource = createKafkaSource(kafkaBootstrap, "ods_order_info", "dwd_order_info");
        KafkaSource<String> paymentInfoSource = createKafkaSource(kafkaBootstrap, "ods_payment_info", "dwd_payment_info");
        KafkaSource<String> consultOrderLinkSource = createKafkaSource(kafkaBootstrap, "ods_consult_order_link", "dwd_consult_order_link");

        DataStream<String> consultLogStream = env.fromSource(consultLogSource, WatermarkStrategy.noWatermarks(), "Consult Log Source");
        DataStream<String> orderInfoStream = env.fromSource(orderInfoSource, WatermarkStrategy.noWatermarks(), "Order Info Source");
        DataStream<String> paymentInfoStream = env.fromSource(paymentInfoSource, WatermarkStrategy.noWatermarks(), "Payment Info Source");
        DataStream<String> consultOrderLinkStream = env.fromSource(consultOrderLinkSource, WatermarkStrategy.noWatermarks(), "Consult Order Link Source");

        // 1. 清洗咨询日志
        SingleOutputStreamOperator<String> cleanedConsultLog = consultLogStream
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSON.parseObject(value);
                    }
                })
                .filter(obj -> obj.getJSONObject("after") != null)
                .map(new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject obj) throws Exception {
                        JSONObject after = obj.getJSONObject("after");
                        JSONObject out = new JSONObject();
                        out.put("consult_id", after.getString("consult_id"));
                        out.put("buyer_id", after.getString("buyer_id"));
                        out.put("product_id", after.getString("product_id"));
                        out.put("shop_id", after.getString("shop_id"));
                        out.put("cs_id", after.getString("cs_id"));
                        out.put("consult_time", after.getString("consult_time"));
                        out.put("event_type", "consult");
                        return out.toJSONString(); // 直接返回字符串
                    }
                });

        // 2. 清洗订单信息
        SingleOutputStreamOperator<String> cleanedOrderInfo = orderInfoStream
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSON.parseObject(value);
                    }
                })
                .filter(obj -> obj.getJSONObject("after") != null)
                .map(new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject obj) throws Exception {
                        JSONObject after = obj.getJSONObject("after");
                        JSONObject out = new JSONObject();
                        out.put("order_id", after.getString("order_id"));
                        out.put("buyer_id", after.getString("buyer_id"));
                        out.put("product_id", after.getString("product_id"));
                        out.put("shop_id", after.getString("shop_id"));
                        out.put("order_time", after.getString("order_time"));
                        out.put("amount", after.getBigDecimal("amount"));
                        out.put("event_type", "order");
                        return out.toJSONString(); // 直接返回字符串
                    }
                });

        // 3. 清洗支付信息
        SingleOutputStreamOperator<String> cleanedPaymentInfo = paymentInfoStream
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSON.parseObject(value);
                    }
                })
                .filter(obj -> obj.getJSONObject("after") != null)
                .map(new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject obj) throws Exception {
                        JSONObject after = obj.getJSONObject("after");
                        JSONObject out = new JSONObject();
                        out.put("payment_id", after.getString("payment_id"));
                        out.put("order_id", after.getString("order_id"));
                        out.put("buyer_id", after.getString("buyer_id"));
                        out.put("payment_time", after.getString("payment_time"));
                        out.put("amount", after.getBigDecimal("amount"));
                        out.put("event_type", "payment");
                        return out.toJSONString(); // 直接返回字符串
                    }
                });

        // 4. 清洗咨询订单关联表
        SingleOutputStreamOperator<String> cleanedConsultOrderLink = consultOrderLinkStream
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSON.parseObject(value);
                    }
                })
                .filter(obj -> obj.getJSONObject("after") != null)
                .map(new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject obj) throws Exception {
                        JSONObject after = obj.getJSONObject("after");
                        JSONObject out = new JSONObject();
                        out.put("consult_id", after.getString("consult_id"));
                        out.put("order_id", after.getString("order_id"));
                        out.put("link_time", after.getString("link_time"));
                        return out.toJSONString(); // 直接返回字符串
                    }
                });

        // 5. 将清洗后的数据写入 Kafka
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaBootstrap);

        FlinkKafkaProducer<String> consultLogSink = new FlinkKafkaProducer<>(
                "dwd_consult_log_02",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> orderInfoSink = new FlinkKafkaProducer<>(
                "dwd_order_info_02",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> paymentInfoSink = new FlinkKafkaProducer<>(
                "dwd_payment_info_02",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> consultOrderLinkSink = new FlinkKafkaProducer<>(
                "dwd_consult_order_link_02",
                new SimpleStringSchema(),
                props
        );

        cleanedConsultLog.addSink(consultLogSink).name("DWD Consult Log Sink");
        cleanedOrderInfo.addSink(orderInfoSink).name("DWD Order Info Sink");
        cleanedPaymentInfo.addSink(paymentInfoSink).name("DWD Payment Info Sink");
        cleanedConsultOrderLink.addSink(consultOrderLinkSink).name("DWD Consult Order Link Sink");

        env.execute("DWD Shop Performance Event App");
    }

    private static KafkaSource<String> createKafkaSource(String bootstrapServers, String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}