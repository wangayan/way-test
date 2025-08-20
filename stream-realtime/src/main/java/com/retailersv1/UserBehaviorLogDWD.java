package com.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * @Title: UserBehaviorLogDWD
 * @Author wang.Ayan
 * @Date 2025/8/20 8:50
 * @Package com.retailers
 * @description:
 * 数据整合与维度关联,将 DWD 层的离散事件流拼接成完整的用户行为事件，并关联 HBase 中的维度信息
 */

/**
 * 用户行为日志 DWD 任务
 * - 读取 Kafka ODS：ods_start_log / ods_page_log / ods_display_log / ods_action_log
 * - 以 common.sid 为 key 做三层 intervalJoin：
 *   1) page + display
 *   2) page_display + action
 *   3) start + (page_display_action)
 * - 异步 I/O 关联 HBase 的维度表 dim_*
 * - 输出到 Kafka：dwd_user_behavior_log
 *
 * 注意：
 * 1) 真实线上请按事件时间打水位，这里示例简单使用 noWatermarks。
 * 2) HBase 列族/字段/表名需按你实际环境修改。
 */
public class UserBehaviorLogDWD {

    public static void main(String[] args) throws Exception {

        // 1) Flink 环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        final String bootstrapServers = "cdh01:9092,cdh02:9092,cdh03:9092";

        // 2) Kafka Sources (新增 ods_action_log)
        KafkaSource<String> startSource = KafkaUtils.buildKafkaSource(
                bootstrapServers, "realtime_start_log", "user_behavior_group", OffsetsInitializer.earliest()
        );
        KafkaSource<String> pageSource = KafkaUtils.buildKafkaSource(
                bootstrapServers, "realtime_page_log", "user_behavior_group", OffsetsInitializer.earliest()
        );
        KafkaSource<String> displaySource = KafkaUtils.buildKafkaSource(
                bootstrapServers, "realtime_display_log", "user_behavior_group", OffsetsInitializer.earliest()
        );
        KafkaSource<String> actionSource = KafkaUtils.buildKafkaSource(
                bootstrapServers, "realtime_action_log", "user_behavior_group", OffsetsInitializer.earliest()
        );

        DataStreamSource<String> startRaw = env.fromSource(startSource, WatermarkStrategy.noWatermarks(), "realtime_start_log");
        DataStreamSource<String> pageRaw = env.fromSource(pageSource, WatermarkStrategy.noWatermarks(), "realtime_page_log");
        DataStreamSource<String> displayRaw = env.fromSource(displaySource, WatermarkStrategy.noWatermarks(), "realtime_display_log");
        DataStreamSource<String> actionRaw = env.fromSource(actionSource, WatermarkStrategy.noWatermarks(), "realtime_action_log");

        // 3) 解析与标准化 (新增 actionJson)
        DataStream<JSONObject> startJson = startRaw
                .map(UserBehaviorLogDWD::safeParse)
                .filter(Objects::nonNull)
                .map(json -> normalize(json, "start"))
                .filter(json -> json.getString("sid") != null);

        DataStream<JSONObject> pageJson = pageRaw
                .map(UserBehaviorLogDWD::safeParse)
                .filter(Objects::nonNull)
                .map(json -> normalize(json, "page"))
                .filter(json -> json.getString("sid") != null);

        DataStream<JSONObject> displayJson = displayRaw
                .map(UserBehaviorLogDWD::safeParse)
                .filter(Objects::nonNull)
                .map(json -> normalize(json, "display"))
                .filter(json -> json.getString("sid") != null);

        DataStream<JSONObject> actionJson = actionRaw
                .map(UserBehaviorLogDWD::safeParse)
                .filter(Objects::nonNull)
                .map(json -> normalize(json, "action"))
                .filter(json -> json.getString("sid") != null);

        // 4) page + display 以 sid 做 intervalJoin（±10s）
        SingleOutputStreamOperator<JSONObject> pageDisplayJoined =
                pageJson
                        .keyBy(json -> json.getString("sid"))
                        .intervalJoin(displayJson.keyBy(json -> json.getString("sid")))
                        .between(Time.milliseconds(-10_000), Time.milliseconds(10_000))
                        .process(new PageDisplayJoinFunction())
                        .name("join_page_display");

        // 5) page_display + action 以 sid 做 intervalJoin（±10s）
        SingleOutputStreamOperator<JSONObject> pageDisplayActionJoined =
                pageDisplayJoined
                        .keyBy(json -> json.getString("sid"))
                        .intervalJoin(actionJson.keyBy(json -> json.getString("sid")))
                        .between(Time.milliseconds(-10_000), Time.milliseconds(10_000))
                        .process(new PageDisplayActionJoinFunction())
                        .name("join_page_display_action");

        // 6) start + (page_display_action) 以 sid 做 intervalJoin（±30s）
        SingleOutputStreamOperator<JSONObject> userBehavior =
                startJson
                        .keyBy(json -> json.getString("sid"))
                        .intervalJoin(pageDisplayActionJoined.keyBy(json -> json.getString("sid")))
                        .between(Time.milliseconds(-30_000), Time.milliseconds(30_000))
                        .process(new StartPageDisplayActionJoinFunction())
                        .name("join_start_page_display_action");

        // 7) 异步 I/O 查 HBase 维度
        SingleOutputStreamOperator<JSONObject> enriched =
                AsyncDataStream.unorderedWait(
                        userBehavior,
                        new HBaseAsyncDimFunction(),
                        30,
                        TimeUnit.SECONDS,
                        100
                ).name("async_hbase_enrich");

        // 8) Kafka Sink: dwd_user_behavior_log
        final String targetTopic = "dwd_user_behavior_log";
        if (!KafkaUtils.kafkaTopicExists(bootstrapServers, targetTopic)) {
            KafkaUtils.createKafkaTopic(bootstrapServers, targetTopic, 6, (short) 1, false);
        }
        KafkaSink<String> sink = KafkaUtils.buildKafkaSink(bootstrapServers, targetTopic);

        enriched.map(obj -> obj.toJSONString()).sinkTo(sink).name("sink_dwd_user_behavior_log");

        env.execute("DWD User Behavior Log Job");
    }

    /* ----------------- 工具：解析 + 规范化 ----------------- */

    private static JSONObject safeParse(String line) {
        try {
            if (line == null || line.trim().isEmpty()) return null;
            return JSON.parseObject(line);
        } catch (Exception e) {
            System.err.println("JSON parse error: " + line);
            return null;
        }
    }

    /**
     * 统一规范：
     * - 把 common 字段拍平到根：sid、uid、mid、ch、os、vc...
     * - 保留原始结构到 event_detail
     * - 增加 event_type、ts
     * - 支持 action 事件类型
     */
    private static JSONObject normalize(JSONObject raw, String eventType) {
        if (raw == null) return null;
        JSONObject out = new JSONObject(true);

        JSONObject common = raw.getJSONObject("common");
        if (common != null) {
            if (common.containsKey("sid")) out.put("sid", common.getString("sid"));
            if (common.containsKey("uid")) out.put("uid", common.getString("uid"));
            if (common.containsKey("mid")) out.put("mid", common.getString("mid"));
            if (common.containsKey("ch")) out.put("ch", common.getString("ch"));
            if (common.containsKey("os")) out.put("os", common.getString("os"));
            if (common.containsKey("vc")) out.put("vc", common.getString("vc"));
            if (common.containsKey("ar")) out.put("ar", common.getString("ar"));
            if (common.containsKey("md")) out.put("md", common.getString("md"));
            if (common.containsKey("ba")) out.put("ba", common.getString("ba"));
        }

        out.put("event_type", eventType);
        out.put("ts", raw.getLongValue("ts"));

        // 事件细节：保留对应部分
        JSONObject detail = new JSONObject(true);
        if ("start".equals(eventType)) {
            JSONObject s = raw.getJSONObject("start");
            if (s != null) detail.putAll(s);
        } else if ("page".equals(eventType)) {
            JSONObject p = raw.getJSONObject("page");
            if (p != null) detail.put("page", p);
            JSONArray displays = raw.getJSONArray("displays");
            if (displays != null) detail.put("displays", displays);
        } else if ("display".equals(eventType)) {
            JSONObject p = raw.getJSONObject("page");
            if (p != null) detail.put("page", p);
            JSONArray displays = raw.getJSONArray("displays");
            if (displays != null) detail.put("displays", displays);
        } else if ("action".equals(eventType)) {
            JSONObject a = raw.getJSONObject("action");
            if (a != null) detail.putAll(a);
        }
        out.put("event_detail", detail);

        // 提取可能的实体 id（便于维度补充）
        String skuId = extractSkuId(raw);
        String actId = extractActivityId(raw);

        // 对于 action 事件，还需要考虑从 action 字段中提取
        if ("action".equals(eventType)) {
            JSONObject action = raw.getJSONObject("action");
            if (action != null) {
                if (skuId == null && action.containsKey("sku_id")) {
                    skuId = action.getString("sku_id");
                }
                if (actId == null && action.containsKey("activity_id")) {
                    actId = action.getString("activity_id");
                }
            }
        }

        if (skuId != null) out.put("sku_id", skuId);
        if (actId != null) out.put("activity_id", actId);

        return out;
    }

    private static String extractSkuId(JSONObject raw) {
        // 优先从 displays 中找 item_type=sku_id
        JSONArray displays = raw.getJSONArray("displays");
        if (displays != null) {
            for (int i = 0; i < displays.size(); i++) {
                JSONObject d = displays.getJSONObject(i);
                if ("sku_id".equalsIgnoreCase(d.getString("item_type"))) {
                    return d.getString("item");
                }
            }
        }
        // 再从 page.item_type 判断
        JSONObject page = raw.getJSONObject("page");
        if (page != null && "sku_id".equalsIgnoreCase(page.getString("item_type"))) {
            return page.getString("item");
        }
        return null;
    }

    private static String extractActivityId(JSONObject raw) {
        // 从 displays 中找 item_type=activity_id
        JSONArray displays = raw.getJSONArray("displays");
        if (displays != null) {
            for (int i = 0; i < displays.size(); i++) {
                JSONObject d = displays.getJSONObject(i);
                if ("activity_id".equalsIgnoreCase(d.getString("item_type"))) {
                    return d.getString("item");
                }
            }
        }
        return null;
    }

    /* ----------------- Join 1：page + display ----------------- */

    /**
     * 把同一 sid、时间接近的 page 事件与 display 事件合并为一条。
     * - page 的信息优先作为主体
     * - display 的 displays 追加合并（去重）
     */
    public static class PageDisplayJoinFunction extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
        @Override
        public void processElement(JSONObject page,
                                   JSONObject display,
                                   Context ctx,
                                   Collector<JSONObject> out) {
            // 复制一份，避免修改下游引用
            JSONObject result = JSON.parseObject(page.toJSONString());

            JSONObject pageDetail = result.getJSONObject("event_detail");
            if (pageDetail == null) {
                pageDetail = new JSONObject(true);
                result.put("event_detail", pageDetail);
            }
            // 合并 display 的 displays
            JSONObject displayDetail = display.getJSONObject("event_detail");
            if (displayDetail != null) {
                JSONArray displayArr = displayDetail.getJSONArray("displays");
                if (displayArr != null && !displayArr.isEmpty()) {
                    JSONArray merged = pageDetail.getJSONArray("displays");
                    if (merged == null) merged = new JSONArray();
                    // 简单去重：item+item_type 合并
                    Set<String> seen = new HashSet<>();
                    for (int i = 0; i < merged.size(); i++) {
                        JSONObject d = merged.getJSONObject(i);
                        seen.add(d.getString("item_type") + "_" + d.getString("item"));
                    }
                    for (int i = 0; i < displayArr.size(); i++) {
                        JSONObject d = displayArr.getJSONObject(i);
                        String key = d.getString("item_type") + "_" + d.getString("item");
                        if (!seen.contains(key)) {
                            merged.add(d);
                            seen.add(key);
                        }
                    }
                    pageDetail.put("displays", merged);
                }
            }

            // 时间取两者较晚的
            long ts = Math.max(result.getLongValue("ts"), display.getLongValue("ts"));
            result.put("ts", ts);

            // 事件类型标注为 page_display
            result.put("event_type", "page_display");

            // sku/activity 补提取
            if (!result.containsKey("sku_id")) {
                String sku = display.getString("sku_id");
                if (sku != null) result.put("sku_id", sku);
            }
            if (!result.containsKey("activity_id")) {
                String act = display.getString("activity_id");
                if (act != null) result.put("activity_id", act);
            }

            out.collect(result);
        }
    }

    /* ----------------- Join 2：page_display + action ----------------- */

    /**
     * 把同一 sid、时间接近的 page_display 事件与 action 事件合并为一条。
     * - page_display 的信息作为主体
     * - action 的信息添加到 event_detail.action
     */
    public static class PageDisplayActionJoinFunction extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
        @Override
        public void processElement(JSONObject pageDisplay,
                                   JSONObject action,
                                   Context ctx,
                                   Collector<JSONObject> out) {
            JSONObject result = JSON.parseObject(pageDisplay.toJSONString());

            // 合并 action 信息
            JSONObject pageDisplayDetail = result.getJSONObject("event_detail");
            if (pageDisplayDetail == null) {
                pageDisplayDetail = new JSONObject(true);
                result.put("event_detail", pageDisplayDetail);
            }

            JSONObject actionDetail = action.getJSONObject("event_detail");
            if (actionDetail != null && !actionDetail.isEmpty()) {
                pageDisplayDetail.put("action", actionDetail);
            }

            // 时间取较晚的
            long ts = Math.max(result.getLongValue("ts"), action.getLongValue("ts"));
            result.put("ts", ts);

            // 事件类型更新
            result.put("event_type", "page_display_action");

            // 补充可能的 sku/activity 信息
            if (!result.containsKey("sku_id") && action.containsKey("sku_id")) {
                result.put("sku_id", action.getString("sku_id"));
            }
            if (!result.containsKey("activity_id") && action.containsKey("activity_id")) {
                result.put("activity_id", action.getString("activity_id"));
            }

            out.collect(result);
        }
    }

    /* ----------------- Join 3：start + (page_display_action) ----------------- */

    /**
     * 把同一 sid 的 start 与 page_display_action 合并为一条用户行为
     * - 保留公共信息（uid/mid/ch...）
     * - 合并 event_detail：start 下的字段写入 event_detail.start
     */
    public static class StartPageDisplayActionJoinFunction extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
        @Override
        public void processElement(JSONObject start,
                                   JSONObject pageDisplayAction,
                                   Context ctx,
                                   Collector<JSONObject> out) {
            JSONObject result = JSON.parseObject(pageDisplayAction.toJSONString());

            // 合并 start 的细节
            JSONObject startDetail = start.getJSONObject("event_detail");
            if (startDetail != null && !startDetail.isEmpty()) {
                JSONObject resDetail = result.getJSONObject("event_detail");
                if (resDetail == null) {
                    resDetail = new JSONObject(true);
                    result.put("event_detail", resDetail);
                }
                resDetail.put("start", startDetail);
            }

            // 公共字段优先保留 pageDisplayAction，有缺再补
            for (String k : Arrays.asList("uid", "mid", "ch", "os", "vc", "ar", "md", "ba")) {
                if (!result.containsKey(k) && start.containsKey(k)) {
                    result.put(k, start.getString(k));
                }
            }

            // ts 取较晚
            result.put("ts", Math.max(start.getLongValue("ts"), pageDisplayAction.getLongValue("ts")));
            result.put("event_type", "start_page_display_action");

            // sku/activity 如果缺，补
            if (!result.containsKey("sku_id") && start.containsKey("sku_id")) {
                result.put("sku_id", start.getString("sku_id"));
            }
            if (!result.containsKey("activity_id") && start.containsKey("activity_id")) {
                result.put("activity_id", start.getString("activity_id"));
            }

            out.collect(result);
        }
    }

    /* ----------------- Async HBase 维度查询 ----------------- */

    /**
     * 通过异步线程池封装 HBase 查询，对以下几个维度尝试补充：
     * - dim_user_info      rowkey=uid     列族 cf：name, gender, age
     * - dim_sku_info       rowkey=sku_id  列族 cf：sku_name, category_id, tm_id
     * - dim_activity_info  rowkey=activity_id  列族 cf：activity_name
     *
     * 请按你实际表/列族/字段调整。
     */
    public static class HBaseAsyncDimFunction extends RichAsyncFunction<JSONObject, JSONObject> {

        private transient org.apache.hadoop.conf.Configuration hconf;
        private transient Connection hConn;
        private transient ExecutorService pool;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            hconf = HBaseConfiguration.create();
            hconf.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
            hconf.set("hbase.zookeeper.property.clientPort", "2181");
            hConn = ConnectionFactory.createConnection(hconf);

            pool = new ThreadPoolExecutor(
                    8, 16, 60L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(1024),
                    r -> {
                        Thread t = new Thread(r);
                        t.setName("hbase-async-pool");
                        t.setDaemon(true);
                        return t;
                    }
            );
        }

        @Override
        public void close() throws Exception {
            if (pool != null) pool.shutdown();
            if (hConn != null) hConn.close();
        }

        @Override
        public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) {
            JSONObject event = JSON.parseObject(input.toJSONString());

            List<Callable<Void>> tasks = new ArrayList<>();

            // 维度：用户
            String uid = event.getString("uid");
            if (uid != null && !uid.isEmpty()) {
                tasks.add(() -> {
                    enrichFromHBase(event, "dim_user_info", uid,
                            new String[][]{
                                    {"cf", "name", "user_name"},
                                    {"cf", "gender", "user_gender"},
                                    {"cf", "age", "user_age"}
                            });
                    return null;
                });
            }

            // 维度：SKU
            String skuId = event.getString("sku_id");
            if (skuId != null && !skuId.isEmpty()) {
                tasks.add(() -> {
                    enrichFromHBase(event, "dim_sku_info", skuId,
                            new String[][]{
                                    {"cf", "sku_name", "sku_name"},
                                    {"cf", "category_id", "category_id"},
                                    {"cf", "tm_id", "tm_id"}
                            });
                    return null;
                });
            }

            // 维度：活动
            String actId = event.getString("activity_id");
            if (actId != null && !actId.isEmpty()) {
                tasks.add(() -> {
                    enrichFromHBase(event, "dim_activity_info", actId,
                            new String[][]{
                                    {"cf", "activity_name", "activity_name"}
                            });
                    return null;
                });
            }

            // 无维度可补，直接返回
            if (tasks.isEmpty()) {
                resultFuture.complete(Collections.singleton(event));
                return;
            }

            // 并发执行
            try {
                List<Future<Void>> futures = pool.invokeAll(tasks, 20, TimeUnit.SECONDS);
                for (Future<Void> f : futures) {
                    try {
                        f.get(0, TimeUnit.MILLISECONDS);
                    } catch (Exception ignore) {
                        // 单个维度失败忽略
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            resultFuture.complete(Collections.singleton(event));
        }

        private void enrichFromHBase(JSONObject event,
                                     String tableName,
                                     String rowKey,
                                     String[][] cfColToAlias) {
            Table table = null;
            try {
                table = hConn.getTable(TableName.valueOf(tableName));
                Get get = new Get(Bytes.toBytes(rowKey));
                Result res = table.get(get);
                if (res != null && !res.isEmpty()) {
                    for (String[] triplet : cfColToAlias) {
                        byte[] val = res.getValue(Bytes.toBytes(triplet[0]), Bytes.toBytes(triplet[1]));
                        if (val != null) {
                            event.put(triplet[2], new String(val, StandardCharsets.UTF_8));
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("HBase enrich error, table=" + tableName + ", rowKey=" + rowKey + ", msg=" + e.getMessage());
            } finally {
                if (table != null) {
                    try { table.close(); } catch (Exception ignore) {}
                }
            }
        }
    }
}