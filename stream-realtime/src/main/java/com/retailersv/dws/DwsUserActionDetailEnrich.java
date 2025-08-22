package com.retailersv.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.HbaseUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * DWS 用户行为明细拉宽：
 * - 消费 dws_user_action_detail（start/page/display/action 的统一明细）
 * - JSON 清洗 + 脏数据侧输出
 * - 异步关联多张 HBase 维表（sku / 品牌 / 类目 / 用户）
 * - 输出 Enriched 明细到 Kafka（dws_user_action_enriched）
 */
public class DwsUserActionDetailEnrich {
    public static void main(String[] args) throws Exception {
        // 1. Flink 环境准备（并行度、重启、checkpoint 等在工具类里统一设置）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-dws"); // 本地测试路径

        // 2. Kafka Source：读取 DWS 明细流
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")                  // Kafka broker
                .setTopics("dws_user_action_detail")                // 输入主题
                .setGroupId("flink_user_action_enrich")             // 消费组
                .setStartingOffsets(OffsetsInitializer.earliest())  // 从最早位置
                .setValueOnlyDeserializer(new SimpleStringSchema()) // value 为纯字符串
                .build();

        // 3. 数据清洗与脏数据侧输出
        OutputTag<JSONObject> dirtyTag = new OutputTag<JSONObject>("dirty") {}; // 侧输出标签：脏数据
        SingleOutputStreamOperator<JSONObject> stream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) {
                        try {
                            // 解析 JSON（不做 schema 校验，异常统一丢到侧输出）
                            JSONObject json = JSONObject.parseObject(value);
                            out.collect(json); // 主流
                        } catch (Exception e) {
                            // 记录原始内容与错误信息到侧输出
                            JSONObject dirty = new JSONObject();
                            dirty.put("raw", value);
                            dirty.put("err", e.getMessage());
                            ctx.output(dirtyTag, dirty);
                        }
                    }
                });

        // 4. 异步关联多个维度（按需可调整顺序或删减）
        //    joinKey 需要与上游明细中的字段一致：sku_id/tm_id/category3_id/uid（注意 uid 与 user_id 命名是否统一）
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_sku_info", "sku_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_base_trademark", "tm_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_base_category3", "category3_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_user_info", "uid");

        // 调试打印（生产可移除或降采样）
        stream.print();

        // 5. 输出主流到 Kafka（Enriched 结果）
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("dws_user_action_enriched")           // 输出主题
                        .setValueSerializationSchema(new SimpleStringSchema()) // value 序列化为 String
                        .build())
                .build();

        stream.map(JSON::toJSONString).sinkTo(kafkaSink);

        // 6. 输出脏数据（便于巡检）
        stream.getSideOutput(dirtyTag).print("dirty");

        // 7. 启动作业
        env.execute("DwsUserActionDetailEnrich");
    }

    /**
     * 通用异步维度拉宽：
     * - 从 input JSON 中提取 joinKey（顶层/common/display）
     * - 使用 HBase Get 查询维度行，合并列为 JSON 字段
     * - 使用 unorderedWait 提升吞吐（乱序完成）
     */
    private static DataStream<JSONObject> asyncJoin(DataStream<JSONObject> input, String tableName, String joinKey) {
        return AsyncDataStream.unorderedWait(
                input,
                new RichAsyncFunction<JSONObject, JSONObject>() {
                    private transient Connection conn; // HBase 连接（生命周期：算子并发实例）
                    private transient Table table;     // HBase 表句柄（按表复用）

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 获取共享连接（建议在 HbaseUtils 中做单例/连接池）
                        conn = HbaseUtils.getConnection();
                        table = conn.getTable(TableName.valueOf(tableName));
                    }

                    @Override
                    public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) {
                        try {
                            // 1) 提取关联键：优先从顶层字段取；否则尝试 common；否则 display（曝光明细）
                            String key = null;
                            if (input.containsKey(joinKey)) {
                                key = input.getString(joinKey); // 顶层：sku_id / tm_id / category3_id / uid
                            } else if (input.containsKey("common")) {
                                key = input.getJSONObject("common").getString(joinKey); // common 中的 id（如 uid）
                            } else if (input.containsKey("display")) {
                                // 曝光结构若是 displays 数组，需在上游打平，这里只是兜底尝试
                                key = input.getJSONObject("display").getString("item");
                            }

                            // 2) joinKey 缺失：直接透传
                            if (key == null || key.trim().isEmpty()) {
                                resultFuture.complete(Collections.singleton(input));
                                return;
                            }

                            final String finalKey = key;

                            // 3) 异步 HBase 查询
                            CompletableFuture
                                    .supplyAsync(() -> {
                                        try {
                                            Get get = new Get(Bytes.toBytes(finalKey));
                                            Result result = table.get(get);
                                            if (!result.isEmpty()) {
                                                // 将列族内所有列转为 JSON，并合入 input
                                                JSONObject dimJson = new JSONObject();
                                                result.listCells().forEach(cell -> {
                                                    String col = Bytes.toString(
                                                            cell.getQualifierArray(),
                                                            cell.getQualifierOffset(),
                                                            cell.getQualifierLength());
                                                    String val = Bytes.toString(
                                                            cell.getValueArray(),
                                                            cell.getValueOffset(),
                                                            cell.getValueLength());
                                                    if (col != null && val != null) {
                                                        // 不加前缀：直接平铺到主 JSON；如需避免覆盖，可改为 "dim_"+col
                                                        dimJson.put(col, val);
                                                    }
                                                });
                                                input.putAll(dimJson);
                                            }
                                        } catch (Exception e) {
                                            // 查询异常：记录错误信息但不中断主流程
                                            input.put(tableName + "_error", e.getMessage());
                                        }
                                        return input;
                                    })
                                    // 4) 异步完成回调：输出单条结果
                                    .thenAccept(res -> resultFuture.complete(Collections.singleton(res)));

                        } catch (Exception e) {
                            // 兜底异常：避免卡住流
                            input.put(tableName + "_exception", e.getMessage());
                            resultFuture.complete(Collections.singleton(input));
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        // 关闭资源（Flink 会按并发度调用）
                        if (table != null) table.close();
                        if (conn != null) conn.close();
                    }
                },
                60, TimeUnit.SECONDS, // 每次异步调用超时（可按 HBase 延迟调整）
                100                   // 最大并发异步请求（与 HBase/QPS/线程池能力匹配）
        );
    }
}
