package com.gd.gd02.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @Title: DimToHBaseApp
 * @Author wang.Ayan
 * @Date 2025/8/28 20:35
 * @Package com.gd.gd02.dim
 * @description: 维度数据写入HBase作业：消费ODS层维度数据，写入HBase维度表
 */
public class DimToHBaseApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);

        // Kafka 配置
        String kafkaBootstrap = "cdh01:9092,cdh02:9092,cdh03:9092";

        // 创建Kafka Source来消费维度数据
        KafkaSource<String> productInfoSource = createKafkaSource(kafkaBootstrap, "ods_product_info", "flink_dim_product");
        KafkaSource<String> shopInfoSource = createKafkaSource(kafkaBootstrap, "ods_shop_info", "flink_dim_shop");
        KafkaSource<String> csInfoSource = createKafkaSource(kafkaBootstrap, "ods_cs_info", "flink_dim_cs");

        // 消费各维度主题数据
        DataStream<String> productInfoStream = env.fromSource(productInfoSource, WatermarkStrategy.noWatermarks(), "Product Info Source");
        DataStream<String> shopInfoStream = env.fromSource(shopInfoSource, WatermarkStrategy.noWatermarks(), "Shop Info Source");
        DataStream<String> csInfoStream = env.fromSource(csInfoSource, WatermarkStrategy.noWatermarks(), "CS Info Source");

        // 处理商品维度数据并写入HBase
        productInfoStream.map(new RichMapFunction<String, String>() {
            private transient Connection hbaseConn;
            private transient Table productTable;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
                config.set("hbase.zookeeper.property.clientPort", "2181");
                hbaseConn = ConnectionFactory.createConnection(config);
                productTable = hbaseConn.getTable(TableName.valueOf("dim_product"));
            }

            @Override
            public String map(String value) {
                try {
                    JSONObject json = JSON.parseObject(value);
                    JSONObject after = json.getJSONObject("after");

                    if (after != null) {
                        String productId = after.getString("product_id");
                        String opType = json.getString("op");

                        // 处理删除操作
                        if ("d".equals(opType)) {
                            // 这里可以添加删除HBase中对应行的逻辑
                            System.out.println("Delete operation detected for product: " + productId);
                            return "Product " + productId + " delete operation";
                        }

                        // 创建HBase Put对象
                        Put put = new Put(Bytes.toBytes(productId));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("product_name"),
                                Bytes.toBytes(after.getString("product_name")));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("category"),
                                Bytes.toBytes(after.getString("category")));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("price"),
                                Bytes.toBytes(after.getBigDecimal("price").toString()));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("create_time"),
                                Bytes.toBytes(after.getString("create_time")));

                        // 写入HBase
                        productTable.put(put);

                        return "Product " + productId + " written to HBase";
                    }
                } catch (Exception e) {
                    System.err.println("Failed to process product info: " + value);
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public void close() throws Exception {
                if (productTable != null) productTable.close();
                if (hbaseConn != null) hbaseConn.close();
                super.close();
            }
        }).print().name("Product Info HBase Sink");

        // 处理店铺维度数据并写入HBase
        shopInfoStream.map(new RichMapFunction<String, String>() {
            private transient Connection hbaseConn;
            private transient Table shopTable;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
                config.set("hbase.zookeeper.property.clientPort", "2181");
                hbaseConn = ConnectionFactory.createConnection(config);
                shopTable = hbaseConn.getTable(TableName.valueOf("dim_shop"));
            }

            @Override
            public String map(String value) throws Exception {
                try {
                    JSONObject json = JSON.parseObject(value);
                    JSONObject after = json.getJSONObject("after");

                    if (after != null) {
                        String shopId = after.getString("shop_id");
                        String opType = json.getString("op");

                        // 处理删除操作
                        if ("d".equals(opType)) {
                            System.out.println("Delete operation detected for shop: " + shopId);
                            return "Shop " + shopId + " delete operation";
                        }

                        Put put = new Put(Bytes.toBytes(shopId));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("shop_name"),
                                Bytes.toBytes(after.getString("shop_name")));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("manager_id"),
                                Bytes.toBytes(after.getString("manager_id")));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("create_time"),
                                Bytes.toBytes(after.getString("create_time")));

                        shopTable.put(put);

                        return "Shop " + shopId + " written to HBase";
                    }
                } catch (Exception e) {
                    System.err.println("Failed to process shop info: " + value);
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public void close() throws Exception {
                if (shopTable != null) shopTable.close();
                if (hbaseConn != null) hbaseConn.close();
                super.close();
            }
        }).print().name("Shop Info HBase Sink");

        // 处理客服维度数据并写入HBase
        csInfoStream.map(new RichMapFunction<String, String>() {
            private transient Connection hbaseConn;
            private transient Table csTable;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
                config.set("hbase.zookeeper.property.clientPort", "2181");
                hbaseConn = ConnectionFactory.createConnection(config);
                csTable = hbaseConn.getTable(TableName.valueOf("dim_cs"));
            }

            @Override
            public String map(String value) throws Exception {
                try {
                    JSONObject json = JSON.parseObject(value);
                    JSONObject after = json.getJSONObject("after");

                    if (after != null) {
                        String csId = after.getString("cs_id");
                        String opType = json.getString("op");

                        // 处理删除操作
                        if ("d".equals(opType)) {
                            System.out.println("Delete operation detected for CS: " + csId);
                            return "CS " + csId + " delete operation";
                        }

                        Put put = new Put(Bytes.toBytes(csId));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cs_name"),
                                Bytes.toBytes(after.getString("cs_name")));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("shop_id"),
                                Bytes.toBytes(after.getString("shop_id")));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("create_time"),
                                Bytes.toBytes(after.getString("create_time")));

                        csTable.put(put);

                        return "CS " + csId + " written to HBase";
                    }
                } catch (Exception e) {
                    System.err.println("Failed to process cs info: " + value);
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public void close() throws Exception {
                if (csTable != null) csTable.close();
                if (hbaseConn != null) hbaseConn.close();
                super.close();
            }
        }).print().name("CS Info HBase Sink");

        env.execute("Dimension Data to HBase App");
    }

    // 创建Kafka Source的辅助方法
    private static KafkaSource<String> createKafkaSource(String bootstrapServers, String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();
    }
}
