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
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/**
 * @Title: DimToHBaseApp
 * @Author wang.Ayan
 * @Date 2025/8/28
 * @Package com.gd.gd02.dim
 * @description: 维度数据写入HBase作业：消费ODS层维度数据，写入HBase维度表（表名后缀_02）
 */
public class DimToHBaseApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);

        // Kafka 配置
        String kafkaBootstrap = "cdh01:9092,cdh02:9092,cdh03:9092";

        // Kafka Source
        KafkaSource<String> productInfoSource = createKafkaSource(kafkaBootstrap, "ods_product_info", "flink_dim_product");
        KafkaSource<String> shopInfoSource = createKafkaSource(kafkaBootstrap, "ods_shop_info", "flink_dim_shop");
        KafkaSource<String> csInfoSource = createKafkaSource(kafkaBootstrap, "ods_cs_info", "flink_dim_cs");
        KafkaSource<String> dateInfoSource = createKafkaSource(kafkaBootstrap, "ods_date_info", "flink_dim_date");

        DataStream<String> productInfoStream = env.fromSource(productInfoSource, WatermarkStrategy.noWatermarks(), "Product Info Source");
        DataStream<String> shopInfoStream = env.fromSource(shopInfoSource, WatermarkStrategy.noWatermarks(), "Shop Info Source");
        DataStream<String> csInfoStream = env.fromSource(csInfoSource, WatermarkStrategy.noWatermarks(), "CS Info Source");
        DataStream<String> dateInfoStream = env.fromSource(dateInfoSource, WatermarkStrategy.noWatermarks(), "Date Info Source");

        // ================= 商品维度 =================
        productInfoStream.map(new RichMapFunction<String, String>() {
            private transient Connection hbaseConn;
            private transient Table productTable;

            @Override
            public void open(Configuration parameters) throws Exception {
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
                config.set("hbase.zookeeper.property.clientPort", "2181");
                hbaseConn = ConnectionFactory.createConnection(config);

                TableName tn = TableName.valueOf("dim_product_02");
                Admin admin = hbaseConn.getAdmin();
                if (!admin.tableExists(tn)) {
                    TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tn);
                    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.of("info");
                    tdb.setColumnFamily(cfd);
                    admin.createTable(tdb.build());
                    System.out.println("✅ 表 dim_product_02 已自动创建");
                } else {
                    System.out.println("⚠️ 表 dim_product_02 已存在");
                }
                admin.close();
                productTable = hbaseConn.getTable(tn);
            }

            @Override
            public String map(String value) {
                try {
                    JSONObject json = JSON.parseObject(value);
                    JSONObject after = json.getJSONObject("after");

                    if (after != null) {
                        String productId = after.getString("product_id");
                        String opType = json.getString("op");

                        if ("d".equals(opType)) {
                            System.out.println("Delete operation detected for product: " + productId);
                            return "Product " + productId + " delete operation";
                        }

                        String priceStr = after.getString("price");
                        String finalPrice;
                        try {
                            finalPrice = new java.math.BigDecimal(priceStr).toString();
                        } catch (Exception e) {
                            finalPrice = priceStr; // fallback
                        }

                        Put put = new Put(Bytes.toBytes(productId));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("product_name"),
                                Bytes.toBytes(after.getString("product_name")));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("category"),
                                Bytes.toBytes(after.getString("category")));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("price"),
                                Bytes.toBytes(finalPrice));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("create_time"),
                                Bytes.toBytes(after.getString("create_time")));

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
            }
        }).print().name("Product Info HBase Sink");

        // ================= 店铺维度 =================
        shopInfoStream.map(new RichMapFunction<String, String>() {
            private transient Connection hbaseConn;
            private transient Table shopTable;

            @Override
            public void open(Configuration parameters) throws Exception {
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
                config.set("hbase.zookeeper.property.clientPort", "2181");
                hbaseConn = ConnectionFactory.createConnection(config);

                TableName tn = TableName.valueOf("dim_shop_02");
                Admin admin = hbaseConn.getAdmin();
                if (!admin.tableExists(tn)) {
                    TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tn);
                    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.of("info");
                    tdb.setColumnFamily(cfd);
                    admin.createTable(tdb.build());
                    System.out.println("✅ 表 dim_shop_02 已自动创建");
                } else {
                    System.out.println("⚠️ 表 dim_shop_02 已存在");
                }
                admin.close();
                shopTable = hbaseConn.getTable(tn);
            }

            @Override
            public String map(String value) {
                try {
                    JSONObject json = JSON.parseObject(value);
                    JSONObject after = json.getJSONObject("after");

                    if (after != null) {
                        String shopId = after.getString("shop_id");
                        String opType = json.getString("op");

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
            }
        }).print().name("Shop Info HBase Sink");

        // ================= 客服维度 =================
        csInfoStream.map(new RichMapFunction<String, String>() {
            private transient Connection hbaseConn;
            private transient Table csTable;

            @Override
            public void open(Configuration parameters) throws Exception {
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
                config.set("hbase.zookeeper.property.clientPort", "2181");
                hbaseConn = ConnectionFactory.createConnection(config);

                TableName tn = TableName.valueOf("dim_cs_02");
                Admin admin = hbaseConn.getAdmin();
                if (!admin.tableExists(tn)) {
                    TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tn);
                    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.of("info");
                    tdb.setColumnFamily(cfd);
                    admin.createTable(tdb.build());
                    System.out.println("✅ 表 dim_cs_02 已自动创建");
                } else {
                    System.out.println("⚠️ 表 dim_cs_02 已存在");
                }
                admin.close();
                csTable = hbaseConn.getTable(tn);
            }

            @Override
            public String map(String value) {
                try {
                    JSONObject json = JSON.parseObject(value);
                    JSONObject after = json.getJSONObject("after");

                    if (after != null) {
                        String csId = after.getString("cs_id");
                        String opType = json.getString("op");

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
            }
        }).print().name("CS Info HBase Sink");

        // ================= 日期维度 =================
        dateInfoStream.map(new RichMapFunction<String, String>() {
            private transient Connection hbaseConn;
            private transient Table dateTable;

            @Override
            public void open(Configuration parameters) throws Exception {
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
                config.set("hbase.zookeeper.property.clientPort", "2181");
                hbaseConn = ConnectionFactory.createConnection(config);

                TableName tn = TableName.valueOf("dim_date_02");
                Admin admin = hbaseConn.getAdmin();
                if (!admin.tableExists(tn)) {
                    TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tn);
                    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.of("info");
                    tdb.setColumnFamily(cfd);
                    admin.createTable(tdb.build());
                    System.out.println("✅ 表 dim_date_02 已自动创建");
                } else {
                    System.out.println("⚠️ 表 dim_date_02 已存在");
                }
                admin.close();
                dateTable = hbaseConn.getTable(tn);
            }

            @Override
            public String map(String value) {
                try {
                    JSONObject json = JSON.parseObject(value);
                    JSONObject after = json.getJSONObject("after");

                    if (after != null) {
                        String dateId = after.getString("date_id");
                        String opType = json.getString("op");

                        if ("d".equals(opType)) {
                            System.out.println("Delete operation detected for Date: " + dateId);
                            return "Date " + dateId + " delete operation";
                        }

                        Put put = new Put(Bytes.toBytes(dateId));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("date_id"),
                                Bytes.toBytes(dateId));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("year"),
                                Bytes.toBytes(after.getString("year")));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("month"),
                                Bytes.toBytes(after.getString("month")));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("day"),
                                Bytes.toBytes(after.getString("day")));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("is_weekend"),
                                Bytes.toBytes(after.getString("is_weekend")));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("is_holiday"),
                                Bytes.toBytes(after.getString("is_holiday")));

                        dateTable.put(put);
                        return "Date " + dateId + " written to HBase";
                    }
                } catch (Exception e) {
                    System.err.println("Failed to process date info: " + value);
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public void close() throws Exception {
                if (dateTable != null) dateTable.close();
                if (hbaseConn != null) hbaseConn.close();
            }
        }).print().name("Date Info HBase Sink");

        env.execute("Dimension Data to HBase App");
    }

    // Kafka Source helper
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
