package com.gd.gd02.ads;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Title: AdsShopPerformanceApp
 * @Author wang.Ayan
 * @Date 2025/8/28
 * @Package com.gd.gd02.ads
 * @description: ADS层：店铺绩效指标计算与存储
 * 工单编号：大数据-用户画像-02-服务主题店铺绩效
 */
public class AdsShopPerformanceApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);

        // 从 DWS 层读取数据（这里简化处理，实际应从 Kafka 或其它存储读取）
        // DataStream<String> dwsStream = env.addSource(...);

        // 模拟数据（实际应用中应从 DWS 层获取）
        DataStream<String> mockData = env.fromElements(
                "{\"shop_id\":\"1001\",\"date\":\"2025-01-15\",\"consult_count\":150,\"consult_user_count\":120,\"order_count\":80,\"payment_count\":70,\"payment_amount\":56000.00}",
                "{\"shop_id\":\"1002\",\"date\":\"2025-01-15\",\"consult_count\":200,\"consult_user_count\":180,\"order_count\":120,\"payment_count\":100,\"payment_amount\":89000.00}"
        );

        // 计算转化率指标
        DataStream<JSONObject> performanceMetrics = mockData.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject data = JSON.parseObject(value);

                int consultCount = data.getInteger("consult_count");
                int consultUserCount = data.getInteger("consult_user_count");
                int orderCount = data.getInteger("order_count");
                int paymentCount = data.getInteger("payment_count");
                double paymentAmount = data.getDouble("payment_amount");

                // 计算转化率
                double consultToOrderRate = consultCount > 0 ? (double) orderCount / consultCount : 0;
                double consultToPaymentRate = consultCount > 0 ? (double) paymentCount / consultCount : 0;
                double orderToPaymentRate = orderCount > 0 ? (double) paymentCount / orderCount : 0;

                // 计算客单价
                double avgOrderValue = paymentCount > 0 ? paymentAmount / paymentCount : 0;

                JSONObject metrics = new JSONObject();
                metrics.put("shop_id", data.getString("shop_id"));
                metrics.put("date", data.getString("date"));
                metrics.put("consult_count", consultCount);
                metrics.put("consult_user_count", consultUserCount);
                metrics.put("order_count", orderCount);
                metrics.put("payment_count", paymentCount);
                metrics.put("payment_amount", paymentAmount);
                metrics.put("consult_to_order_rate", consultToOrderRate);
                metrics.put("consult_to_payment_rate", consultToPaymentRate);
                metrics.put("order_to_payment_rate", orderToPaymentRate);
                metrics.put("avg_order_value", avgOrderValue);
                metrics.put("update_time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

                return metrics;
            }
        });

        // 写入 HBase
        performanceMetrics.addSink(new RichSinkFunction<JSONObject>() {
            private transient Connection connection;
            private transient Table table;

            @Override
            public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                super.open(parameters);
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
                config.set("hbase.zookeeper.property.clientPort", "2181");
                connection = ConnectionFactory.createConnection(config);
                table = connection.getTable(TableName.valueOf("ads_shop_performance"));
            }

            @Override
            public void invoke(JSONObject value, Context context) throws Exception {
                String shopId = value.getString("shop_id");
                String date = value.getString("date");
                String rowKey = shopId + "_" + date;

                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("consult_count"),
                        Bytes.toBytes(String.valueOf(value.getInteger("consult_count"))));
                put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("consult_user_count"),
                        Bytes.toBytes(String.valueOf(value.getInteger("consult_user_count"))));
                put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("order_count"),
                        Bytes.toBytes(String.valueOf(value.getInteger("order_count"))));
                put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("payment_count"),
                        Bytes.toBytes(String.valueOf(value.getInteger("payment_count"))));
                put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("payment_amount"),
                        Bytes.toBytes(String.valueOf(value.getDouble("payment_amount"))));
                put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("consult_to_order_rate"),
                        Bytes.toBytes(String.valueOf(value.getDouble("consult_to_order_rate"))));
                put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("consult_to_payment_rate"),
                        Bytes.toBytes(String.valueOf(value.getDouble("consult_to_payment_rate"))));
                put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("order_to_payment_rate"),
                        Bytes.toBytes(String.valueOf(value.getDouble("order_to_payment_rate"))));
                put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("avg_order_value"),
                        Bytes.toBytes(String.valueOf(value.getDouble("avg_order_value"))));
                put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("update_time"),
                        Bytes.toBytes(value.getString("update_time")));

                table.put(put);
            }

            @Override
            public void close() throws Exception {
                if (table != null) table.close();
                if (connection != null) connection.close();
                super.close();
            }
        });

        // 输出到控制台
        performanceMetrics.print().name("ADS Shop Performance Output");

        env.execute("ADS Shop Performance App");
    }
}