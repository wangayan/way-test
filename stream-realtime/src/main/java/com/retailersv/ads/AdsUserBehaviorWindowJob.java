//package com.retailersv.ads;
//
//import com.retailersv.bean.UserDisplayClickWindow;
//import com.retailersv.bean.UserPvUvWindow;
//import com.retailersv.bean.UserSessionWindow;
////import com.stream.common.utils.ClickHouseUtil;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//
///**
// * ADS 层 - 用户行为窗口聚合作业
// * 1) 消费 dws_user_action_enriched（已打平 & 拉宽后的用户行为统一流）
// * 2) 计算 10s 窗口的 PV/UV、会话指标、曝光/点击指标
// * 3) 写入 ClickHouse：ads_user_pv_uv_window / ads_user_session_window / ads_user_display_click_window
// */
//public class AdsUserBehaviorWindowJob {
//    public static void main(String[] args) throws Exception {
//
//        // 1. 初始化环境：Flink 流环境 & Table 环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//        // Checkpoint 存储目录（本地演示用；生产建议 HDFS/对象存储）
//        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-ads");
//        env.setParallelism(2); // 并行度（按集群资源调整）
//
//        // Table planner 参数微调（窗口聚合内存 & 默认并行度）
//        tableEnv.getConfig().getConfiguration().setString("table.exec.window-agg.buffer-size.limit", "2000000");
//        tableEnv.getConfig().getConfiguration().setString("table.exec.resource.default-parallelism", "2");
//
//        // 2. 创建 Kafka 源表：消费 DWS 明细（统一 schema）
//        // 说明：
//        //  - 使用 proctime 做窗口演示；生产建议使用事件时间 + WATERMARK
//        tableEnv.executeSql(
//                "CREATE TABLE dws_user_action_enriched (\n" +
//                        "  event_type STRING,\n" +
//                        "  common ROW<ar STRING, uid STRING, ch STRING, is_new STRING, vc STRING, ba STRING, os STRING, md STRING, mid STRING, sid STRING>,\n" +
//                        "  page ROW<page_id STRING, last_page_id STRING, during_time BIGINT>,\n" +
//                        "  display ROW<item STRING, item_type STRING, pos_id INT>,\n" +
//                        "  ts BIGINT,\n" +
//                        "  proctime AS PROCTIME()\n" +   // 处理时间属性（演示用）
//                        ") WITH (\n" +
//                        "  'connector' = 'kafka',\n" +
//                        "  'topic' = 'dws_user_action_enriched',\n" +
//                        "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
//                        "  'properties.group.id' = 'ads_user_behavior_window',\n" +
//                        "  'scan.startup.mode' = 'earliest-offset',\n" +
//                        "  'format' = 'json',\n" +
//                        "  'json.ignore-parse-errors' = 'true'\n" +
//                        ")"
//        );
//
//        // 如需调试原始输入，可解开以下两行
//        // Table origin = tableEnv.sqlQuery("SELECT * FROM dws_user_action_enriched");
//        // tableEnv.toDataStream(origin).print("原始数据");
//
//        // 3. PV/UV 10s 窗口统计（按版本/渠道/地区/新老访客分组）
//        // 说明：
//        //  - pv_ct：窗口内记录数（行数）
//        //  - uv_ct：窗口内去重用户数（distinct uid）
//        Table pvUvTable = tableEnv.sqlQuery(
//                "SELECT\n" +
//                        "  DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt,\n" +
//                        "  DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt,\n" +
//                        "  common.vc, common.ch, common.ar, common.is_new,\n" +
//                        "  COUNT(*) AS pv_ct,\n" +
//                        "  COUNT(DISTINCT common.uid) AS uv_ct\n" +
//                        "FROM TABLE(\n" +
//                        "  TUMBLE(TABLE dws_user_action_enriched, DESCRIPTOR(proctime), INTERVAL '10' SECOND)\n" +
//                        ")\n" +
//                        "GROUP BY window_start, window_end, common.vc, common.ch, common.ar, common.is_new"
//        );
//        // 控制台预览（生产可关闭）
//        tableEnv.toDataStream(pvUvTable).print("pv_uv");
//
//        // 4. 会话指标 10s 窗口（以启动事件为准）
//        // 说明：
//        //  - sv_ct：启动事件次数（近似会话数）
//        //  - uj_ct：跳出会话数（page.last_page_id IS NULL 视为着陆页，无前页 → 近似跳出）
//        Table sessionTable = tableEnv.sqlQuery(
//                "SELECT\n" +
//                        "  DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt,\n" +
//                        "  DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt,\n" +
//                        "  common.vc, common.ch, common.ar, common.is_new,\n" +
//                        "  COUNT(*) AS sv_ct,\n" +
//                        "  SUM(CASE WHEN page.last_page_id IS NULL THEN 1 ELSE 0 END) AS uj_ct\n" +
//                        "FROM TABLE(\n" +
//                        "  TUMBLE(TABLE dws_user_action_enriched, DESCRIPTOR(proctime), INTERVAL '10' SECOND)\n" +
//                        ")\n" +
//                        "WHERE event_type = 'start'\n" +
//                        "GROUP BY window_start, window_end, common.vc, common.ch, common.ar, common.is_new"
//        );
//        tableEnv.toDataStream(sessionTable).print("session");
//
//        // 5. 曝光/点击 10s 窗口（以 SKU、页面、渠道为维度）
//        // 说明：
//        //  - disp_ct：曝光条数
//        //  - disp_sku_num：窗口内曝光过的去重 SKU 数（以 display.item 判定）
//        //  - 这里 WHERE 限定了 event_type IN ('display', 'click')，但示例并未单独输出 click 计数，如需 CTR 可扩展
//        Table displayClickTable = tableEnv.sqlQuery(
//                "SELECT\n" +
//                        "  DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt,\n" +
//                        "  DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt,\n" +
//                        "  display.item AS sku_id,\n" +
//                        "  page.page_id,\n" +
//                        "  common.ch,\n" +
//                        "  COUNT(CASE WHEN event_type = 'display' THEN 1 END) AS disp_ct,\n" +
//                        "  COUNT(DISTINCT CASE WHEN event_type = 'display' THEN display.item END) AS disp_sku_num\n" +
//                        "FROM TABLE(\n" +
//                        "  TUMBLE(TABLE dws_user_action_enriched, DESCRIPTOR(proctime), INTERVAL '10' SECOND)\n" +
//                        ")\n" +
//                        " WHERE event_type IN ('display', 'click') AND display.item IS NOT NULL\n " +
//                        "GROUP BY window_start, window_end, display.item, page.page_id, common.ch"
//        );
//        tableEnv.toDataStream(displayClickTable).print("disp_click");
//
//        // 6. 将三类窗口结果映射为 POJO 并写入 ClickHouse
//        // 6.1 PV/UV -> ads_user_pv_uv_window
//        DataStream<UserPvUvWindow> pvUvStream = tableEnv.toDataStream(pvUvTable, UserPvUvWindow.class);
//        pvUvStream.addSink(ClickHouseUtil.getSink(
//                "INSERT INTO ads_user_pv_uv_window (stt, edt, vc, ch, ar, is_new, pv_ct, uv_ct) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
//        ));
//
//        // 6.2 会话 -> ads_user_session_window
//        DataStream<UserSessionWindow> sessionStream = tableEnv.toDataStream(sessionTable, UserSessionWindow.class);
//        sessionStream.addSink(ClickHouseUtil.getSink(
//                "INSERT INTO ads_user_session_window (stt, edt, vc, ch, ar, is_new, sv_ct, uj_ct) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
//        ));
//
//        // 6.3 曝光/点击 -> ads_user_display_click_window
//        DataStream<UserDisplayClickWindow> dispClickStream = tableEnv.toDataStream(displayClickTable, UserDisplayClickWindow.class);
//        dispClickStream.addSink(ClickHouseUtil.getSink(
//                "INSERT INTO ads_user_display_click_window (stt, edt, sku_id, page_id, ch, disp_ct, disp_sku_num) VALUES (?, ?, ?, ?, ?, ?, ?)"
//        ));
//
//        // 7. 启动作业
//        env.execute("AdsUserBehaviorWindowJob");
//    }
//}
