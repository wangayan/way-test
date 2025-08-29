package com.gd.gd02.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.time.Duration;
import java.util.Properties;

/**
 * @Title: DwsShopPerformanceApp
 * @Author wang.Ayan
 * @Date 2025/8/28
 * @Package com.gd.gd02.dws
 * @description: DWS层：店铺绩效聚合计算
 * 工单编号：大数据-用户画像-02-服务主题店铺绩效
 */
public class DwsShopPerformanceApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);

        String kafkaBootstrap = "cdh01:9092,cdh02:9092,cdh03:9092";

        // 消费 DWD 层 topic
        KafkaSource<String> consultLogSource = createKafkaSource(kafkaBootstrap, "dwd_consult_log_02", "dws_shop_performance");
        KafkaSource<String> orderInfoSource = createKafkaSource(kafkaBootstrap, "dwd_order_info_02", "dws_shop_performance");
        KafkaSource<String> paymentInfoSource = createKafkaSource(kafkaBootstrap, "dwd_payment_info_02", "dws_shop_performance");
        KafkaSource<String> consultOrderLinkSource = createKafkaSource(kafkaBootstrap, "dwd_consult_order_link_02", "dws_shop_performance");

        // 添加水位线策略
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> {
                    JSONObject json = JSON.parseObject(event);
                    if (json.containsKey("consult_time")) {
                        return DateUtil.parseToTimestamp(json.getString("consult_time"));
                    } else if (json.containsKey("order_time")) {
                        return DateUtil.parseToTimestamp(json.getString("order_time"));
                    } else if (json.containsKey("payment_time")) {
                        return DateUtil.parseToTimestamp(json.getString("payment_time"));
                    }
                    return System.currentTimeMillis();
                });

        DataStream<String> consultLogStream = env.fromSource(consultLogSource, watermarkStrategy, "DWD Consult Log Source");
        DataStream<String> orderInfoStream = env.fromSource(orderInfoSource, watermarkStrategy, "DWD Order Info Source");
        DataStream<String> paymentInfoStream = env.fromSource(paymentInfoSource, watermarkStrategy, "DWD Payment Info Source");
        DataStream<String> consultOrderLinkStream = env.fromSource(consultOrderLinkSource, watermarkStrategy, "DWD Consult Order Link Source");

        // 解析 JSON
        SingleOutputStreamOperator<JSONObject> consultLogJson = consultLogStream.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

        SingleOutputStreamOperator<JSONObject> orderInfoJson = orderInfoStream.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

        SingleOutputStreamOperator<JSONObject> paymentInfoJson = paymentInfoStream.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

        SingleOutputStreamOperator<JSONObject> consultOrderLinkJson = consultOrderLinkStream.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

        // 调试输出
        consultLogJson.print().name("Consult Log JSON");
        orderInfoJson.print().name("Order Info JSON");
        paymentInfoJson.print().name("Payment Info JSON");

        // 按店铺和日期分组
        KeyedStream<JSONObject, String> consultLogKeyed = consultLogJson.keyBy(obj -> {
            String shopId = obj.getString("shop_id");
            String consultTime = obj.getString("consult_time");
            String date = consultTime.split(" ")[0]; // 提取日期部分
            return shopId + "_" + date;
        });

        KeyedStream<JSONObject, String> orderInfoKeyed = orderInfoJson.keyBy(obj -> {
            String shopId = obj.getString("shop_id");
            String orderTime = obj.getString("order_time");
            String date = orderTime.split(" ")[0]; // 提取日期部分
            return shopId + "_" + date;
        });

        KeyedStream<JSONObject, String> paymentInfoKeyed = paymentInfoJson.keyBy(obj -> {
            String shopId = obj.getString("shop_id");
            String paymentTime = obj.getString("payment_time");
            String date = paymentTime.split(" ")[0]; // 提取日期部分
            return shopId + "_" + date;
        });

        // 窗口聚合计算
        SingleOutputStreamOperator<JSONObject> consultStats = consultLogKeyed
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new ConsultAggregateFunction());

        SingleOutputStreamOperator<JSONObject> orderStats = orderInfoKeyed
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new OrderAggregateFunction());

        SingleOutputStreamOperator<JSONObject> paymentStats = paymentInfoKeyed
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new PaymentAggregateFunction());

        // 调试输出
        consultStats.print().name("Consult Stats");
        orderStats.print().name("Order Stats");
        paymentStats.print().name("Payment Stats");

        // 关联咨询和订单数据
        SingleOutputStreamOperator<JSONObject> consultOrderStats = consultOrderLinkJson
                .keyBy(obj -> obj.getString("consult_id"))
                .connect(consultLogJson.keyBy(obj -> obj.getString("consult_id")))
                .process(new ConsultOrderLinkFunction());

        consultOrderStats.print().name("Consult Order Stats");

        // 合并所有统计结果
        DataStream<JSONObject> mergedStats = consultStats
                .union(orderStats, paymentStats, consultOrderStats)
                .keyBy(obj -> obj.getString("shop_id") + "_" + obj.getString("date"))
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new ShopPerformanceAggregateFunction());

        mergedStats.print().name("Merged Stats");

        // 创建KafkaSink (新版API)
        KafkaSink<String> shopPerformanceSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("dws_shop_performance")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        // 将聚合结果写入Kafka
        mergedStats.map(obj -> {
                    System.out.println("Writing to Kafka: " + obj.toJSONString()); // 添加日志
                    return obj.toJSONString();
                })
                .sinkTo(shopPerformanceSink)
                .name("Shop Performance Kafka Sink");

        env.execute("DWS Shop Performance App");
    }

    private static KafkaSource<String> createKafkaSource(String bootstrapServers, String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();
    }

    // 添加时间戳工具类
    static class DateUtil {
        public static long parseToTimestamp(String dateTime) {
            try {
                java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                return sdf.parse(dateTime).getTime();
            } catch (Exception e) {
                return System.currentTimeMillis();
            }
        }
    }
}