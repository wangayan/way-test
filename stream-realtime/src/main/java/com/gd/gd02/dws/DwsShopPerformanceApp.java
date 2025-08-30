package com.gd.gd02.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * @Title: DwsShopPerformanceApp
 * @Author wang.Ayan
 * @Date 2025/8/29
 * @Package com.gd.gd02.dws
 * @description: DWS层：轻度聚合DWD层数据，构建店铺绩效服务宽表
 * 工单编号：大数据-用户画像-02-服务主题店铺绩效
 */
public class DwsShopPerformanceApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);

        String kafkaBootstrap = "cdh01:9092,cdh02:9092,cdh03:9092";

        // 消费DWD层topic - 使用latest()而不是earliest()
        KafkaSource<String> consultLogSource = createKafkaSource(kafkaBootstrap, "dwd_consult_log_02", "dws_shop_performance");
        KafkaSource<String> orderInfoSource = createKafkaSource(kafkaBootstrap, "dwd_order_info_02", "dws_shop_performance");
        KafkaSource<String> paymentInfoSource = createKafkaSource(kafkaBootstrap, "dwd_payment_info_02", "dws_shop_performance");
        KafkaSource<String> consultOrderLinkSource = createKafkaSource(kafkaBootstrap, "dwd_consult_order_link_02", "dws_shop_performance");

        DataStream<String> consultLogStream = env.fromSource(consultLogSource, WatermarkStrategy.noWatermarks(), "DWD Consult Log Source");
        DataStream<String> orderInfoStream = env.fromSource(orderInfoSource, WatermarkStrategy.noWatermarks(), "DWD Order Info Source");
        DataStream<String> paymentInfoStream = env.fromSource(paymentInfoSource, WatermarkStrategy.noWatermarks(), "DWD Payment Info Source");
        DataStream<String> consultOrderLinkStream = env.fromSource(consultOrderLinkSource, WatermarkStrategy.noWatermarks(), "DWD Consult Order Link Source");

        // 打印输入数据，用于调试
        consultLogStream.print("Consult Log Input").name("Consult Log Debug");
        orderInfoStream.print("Order Info Input").name("Order Info Debug");
        paymentInfoStream.print("Payment Info Input").name("Payment Info Debug");
        consultOrderLinkStream.print("Consult Order Link Input").name("Consult Order Link Debug");

        // 1. 处理咨询数据，提取时间字段作为事件时间
        SingleOutputStreamOperator<JSONObject> consultEventStream = consultLogStream
                .map((MapFunction<String, JSONObject>) JSON::parseObject)
                .filter(obj -> obj.containsKey("consult_time")) // 确保有时间字段
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> {
                            String consultTime = event.getString("consult_time");
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                            LocalDateTime dateTime = LocalDateTime.parse(consultTime, formatter);
                            return dateTime.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
                        }));

        // 2. 处理订单数据，提取时间字段作为事件时间
        SingleOutputStreamOperator<JSONObject> orderEventStream = orderInfoStream
                .map((MapFunction<String, JSONObject>) JSON::parseObject)
                .filter(obj -> obj.containsKey("order_time")) // 确保有时间字段
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> {
                            String orderTime = event.getString("order_time");
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                            LocalDateTime dateTime = LocalDateTime.parse(orderTime, formatter);
                            return dateTime.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
                        }));

        // 3. 处理支付数据，提取时间字段作为事件时间
        SingleOutputStreamOperator<JSONObject> paymentEventStream = paymentInfoStream
                .map((MapFunction<String, JSONObject>) JSON::parseObject)
                .filter(obj -> obj.containsKey("payment_time")) // 确保有时间字段
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> {
                            String paymentTime = event.getString("payment_time");
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                            LocalDateTime dateTime = LocalDateTime.parse(paymentTime, formatter);
                            return dateTime.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
                        }));

        // 4. 处理咨询订单关联数据
        SingleOutputStreamOperator<JSONObject> consultOrderLinkEventStream = consultOrderLinkStream
                .map((MapFunction<String, JSONObject>) JSON::parseObject);

        // 5. 按商品维度聚合咨询数据
        SingleOutputStreamOperator<String> productConsultAgg = consultEventStream
                .keyBy(event -> event.getString("product_id"))
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new ProductConsultAggregateFunction())
                .map((MapFunction<JSONObject, String>) obj -> obj.toJSONString());

        // 6. 按商品维度聚合订单数据
        SingleOutputStreamOperator<String> productOrderAgg = orderEventStream
                .keyBy(event -> event.getString("product_id"))
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new ProductOrderAggregateFunction())
                .map((MapFunction<JSONObject, String>) obj -> obj.toJSONString());

        // 7. 按商品维度聚合支付数据 - 改进关联逻辑
        SingleOutputStreamOperator<String> productPaymentAgg = paymentEventStream
                .keyBy(event -> {
                    // 实际应用中需要通过订单ID找到对应的商品ID
                    // 这里简化处理，实际需要关联订单表获取商品ID
                    String orderId = event.getString("order_id");
                    // 这里应该从订单流中查找对应的商品ID
                    // 暂时返回订单ID作为占位符
                    return orderId != null ? orderId : "unknown_product";
                })
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new ProductPaymentAggregateFunction())
                .map((MapFunction<JSONObject, String>) obj -> obj.toJSONString());

        // 打印聚合结果，用于调试
        productConsultAgg.print("Product Consult Agg").name("Product Consult Debug");
        productOrderAgg.print("Product Order Agg").name("Product Order Debug");
        productPaymentAgg.print("Product Payment Agg").name("Product Payment Debug");

        // 8. 合并商品维度的咨询、订单、支付数据
        DataStream<String> mergedProductStream = productConsultAgg
                .union(productOrderAgg, productPaymentAgg)
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        try {
                            JSONObject json = JSON.parseObject(value);
                            // 这里实现合并逻辑，将相同商品ID的数据合并到一个JSON对象中
                            out.collect(json.toJSONString());
                        } catch (Exception e) {
                            System.err.println("Failed to merge data: " + value);
                            e.printStackTrace();
                        }
                    }
                });

        // 打印最终结果，用于调试
        mergedProductStream.print("Merged Product Stream").name("Merged Product Debug");

        // 9. 将聚合结果写入Kafka
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaBootstrap);
        props.setProperty("transaction.timeout.ms", "900000"); // 增加事务超时时间

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(new org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder<String>()
                        .setTopic("dws_shop_performance_02")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setKafkaProducerConfig(props)
                .build();

        mergedProductStream.sinkTo(sink).name("DWS Shop Performance Sink");

        env.execute("DWS Shop Performance App");
    }

    private static KafkaSource<String> createKafkaSource(String bootstrapServers, String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest()) // 改为latest()，从最新偏移量开始消费
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}