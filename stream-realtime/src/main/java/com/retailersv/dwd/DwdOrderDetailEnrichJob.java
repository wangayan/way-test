package com.retailersv.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.HbaseUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 订单明细宽表 enrich 作业：
 * 1. 从 Kafka 读取 DWD 层宽表数据（order_detail_wide）
 * 2. JSON 解析 & 脏数据分流
 * 3. 异步关联 HBase 维度表，补充维度信息
 * 4. 字段归一化（金额类转 Double）
 * 5. 输出到 Kafka enriched topic
 */
public class DwdOrderDetailEnrichJob {

    // 定义侧输出流标签：存放 JSON 解析失败或异常数据
    private static final OutputTag<JSONObject> dirtyTag = new OutputTag<JSONObject>("dirty-data") {};

    public static void main(String[] args) throws Exception {
        // 1. 初始化 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-dwd");

        // 2. 定义 Kafka Source，读取 dwd_order_detail_wide_kafka
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("dwd_order_detail_wide_kafka")  // 输入主题
                .setGroupId("flink_order_detail_enrich")   // 消费组
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SafeStringSchema()) // 自定义反序列化
                .build();

        // 3. 消费 Kafka 数据，转换为 JSONObject，同时分流脏数据
        SingleOutputStreamOperator<JSONObject> stream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .filter(line -> line != null && !line.trim().isEmpty())
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) {
                        try {
                            JSONObject json = JSONObject.parseObject(value);
                            if (json == null || json.isEmpty()) {
                                throw new RuntimeException("Empty JSON");
                            }
                            out.collect(json);  // 解析成功 → 主流
                        } catch (Exception e) {
                            JSONObject dirty = new JSONObject();
                            dirty.put("raw", value);
                            dirty.put("error", "JSON解析失败: " + e.getMessage());
                            ctx.output(dirtyTag, dirty); // 解析失败 → 侧输出流
                        }
                    }
                });

        // 4. 异步维度关联 enrich（依次关联 6 张维度表）
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_sku_info", "sku_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_spu_info", "spu_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_base_category3", "category3_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_base_trademark", "tm_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_user_info", "user_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_base_province", "province_id");

        // 5. 字段归一化处理（金额等数值字段）
        stream = stream.map(DwdOrderDetailEnrichJob::normalizeFields);

        // 调试打印 enrich 后数据
        stream.print("enriched");

        // 6. 过滤掉 order_id 为空的数据，避免写入无效消息
        DataStream<JSONObject> filtered = stream
                .filter(json -> json.getString("order_id") != null);

        // 7. 定义 Kafka Sink，输出 enriched 数据到 Kafka
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("dwd_order_detail_enriched") // 输出主题
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setKafkaProducerConfig(new Properties() {{
                    put(ProducerConfig.ACKS_CONFIG, "all"); // 强一致性
                }})
                .build();

        filtered.map(JSON::toJSONString).sinkTo(kafkaSink);

        // 8. 输出脏数据（侧输出流）
        stream.getSideOutput(dirtyTag).print("dirty");

        // 9. 启动作业
        env.execute("DwdOrderDetailEnrichJob");
    }

    /**
     * 自定义 Kafka 反序列化：将 byte[] 转换为 String
     */
    public static class SafeStringSchema implements DeserializationSchema<String> {
        @Override
        public String deserialize(byte[] message) {
            return message == null ? null : new String(message);
        }

        @Override
        public boolean isEndOfStream(String nextElement) {
            return false;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    /**
     * 字段归一化：金额类字段转 Double
     */
    private static JSONObject normalizeFields(JSONObject json) {
        try {
            if (json.containsKey("order_price")) {
                json.put("order_amount", Double.parseDouble(json.getString("order_price")));
            }
            if (json.containsKey("pay_amount")) {
                json.put("payment_amount", Double.parseDouble(json.getString("pay_amount")));
            }
        } catch (Exception e) {
            json.put("cast_error", e.getMessage()); // 转换失败时记录错误
        }
        return json;
    }

    /**
     * 通用异步维度 Join 方法
     * @param input     主流数据
     * @param tableName 维度表名
     * @param joinKey   关联字段
     */
    private static DataStream<JSONObject> asyncJoin(DataStream<JSONObject> input, String tableName, String joinKey) {
        return AsyncDataStream.unorderedWait(
                input,
                new RichAsyncFunction<JSONObject, JSONObject>() {
                    private transient Connection conn; // HBase 连接
                    private transient Table table;     // HBase 表对象

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        conn = HbaseUtils.getConnection();
                        table = conn.getTable(TableName.valueOf(tableName));
                    }

                    @Override
                    public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) {
                        try {
                            String key = input.getString(joinKey);
                            if (key == null || key.trim().isEmpty()) {
                                // joinKey 缺失 → 不做关联
                                resultFuture.complete(Collections.singleton(input));
                                return;
                            }

                            // 异步查询 HBase 维度表
                            CompletableFuture
                                    .supplyAsync(() -> {
                                        try {
                                            Get get = new Get(Bytes.toBytes(key));
                                            Result result = table.get(get);
                                            if (!result.isEmpty()) {
                                                JSONObject dimJson = new JSONObject();
                                                // 遍历所有列，拼接成 JSON
                                                result.listCells().forEach(cell -> {
                                                    String col = Bytes.toString(cell.getQualifierArray(),
                                                            cell.getQualifierOffset(), cell.getQualifierLength());
                                                    String val = Bytes.toString(cell.getValueArray(),
                                                            cell.getValueOffset(), cell.getValueLength());
                                                    if (col != null && val != null) {
                                                        dimJson.put("dim_" + col, val);
                                                    }
                                                });
                                                // 将维度字段合并到主流 JSON
                                                input.putAll(dimJson);
                                            }
                                        } catch (Exception e) {
                                            input.put(tableName + "_error", e.getMessage());
                                        }
                                        return input;
                                    })
                                    .thenAccept(res -> resultFuture.complete(Collections.singleton(res)));
                        } catch (Exception e) {
                            input.put(tableName + "_exception", e.getMessage());
                            resultFuture.complete(Collections.singleton(input));
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        if (table != null) table.close();
                        if (conn != null) conn.close();
                    }
                },
                60, TimeUnit.SECONDS, // 超时时间
                100                   // 最大并发异步请求数
        );
    }
}
