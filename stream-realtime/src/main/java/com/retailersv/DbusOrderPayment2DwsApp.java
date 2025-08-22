package com.retailersv;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.HbaseUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class DbusOrderPayment2DwsApp {

    private static final String CDH_KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        env.enableCheckpointing(10000L);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-chk");

        // ============= 支付流 KafkaSource =============
        KafkaSource<String> paymentSource = KafkaSource.<String>builder()
                .setBootstrapServers(CDH_KAFKA_SERVER)
                .setTopics("dwd_payment_info")
                .setGroupId("dws_order_payment_group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> paymentStream = env.fromSource(paymentSource,
                WatermarkStrategy.noWatermarks(),
                "paymentSource");

        // ============= JSON 转换 + 过滤 =============
        SingleOutputStreamOperator<JSONObject> paymentJson = paymentStream
                .map(JSON::parseObject)
                .filter(json -> json.getString("payment_id") != null);

        // ============= 补充维度信息 =============
        SingleOutputStreamOperator<JSONObject> enrichedPayment = paymentJson.map(json -> {
            String userId = json.getString("user_id");
            String skuId = json.getString("sku_id");

            // 从 HBase 查维度
            JSONObject userDim = HbaseUtils.getRow("ns_zxn:dim_user_info", userId, "info");
            JSONObject skuDim = HbaseUtils.getRow("ns_zxn:dim_sku_info", skuId, "info");

            json.put("user_gender", userDim.getString("gender"));
            json.put("user_age", userDim.getString("age"));
            json.put("sku_name", skuDim.getString("sku_name"));
            json.put("category", skuDim.getString("category3_id"));
            return json;
        });

        // ============= 5分钟窗口统计 GMV & 支付人数 =============
        enrichedPayment
                .map(json -> {
                    JSONObject result = new JSONObject();
                    result.put("amount", json.getDoubleValue("amount")); // 支付金额字段
                    result.put("user_id", json.getString("user_id"));
                    return result;
                })
                .returns(JSONObject.class)
                .keyBy(json -> "all")  // 全局聚合，也可以按店铺/类目分组
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                .reduce((json1, json2) -> {
                    double totalAmount = json1.getDoubleValue("amount") + json2.getDoubleValue("amount");
                    json1.put("amount", totalAmount);

                    // 用 Set 模拟用户去重（这里简化，用字符串拼接）
                    String users = json1.getString("user_id") + "," + json2.getString("user_id");
                    json1.put("user_id", users);
                    return json1;
                })
                .map(json -> {
                    String users = json.getString("user_id");
                    int userCount = users.split(",").length; // 简单模拟
                    JSONObject res = new JSONObject();
                    res.put("window_gmv", json.getDouble("amount"));
                    res.put("window_user_count", userCount);
                    return res;
                })
                .print("5分钟窗口聚合结果");

        env.execute("DbusOrderPayment2DwsApp");
    }
}
