package com.retailersv.dwd;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 订单明细 DWD 宽表构建 Job
 * 功能：
 * 1. 从 Kafka (Debezium 格式) 读取 ODS 订单相关表
 * 2. 按表名拆分成多张临时表（order_detail/order_info/payment/refund 等）
 * 3. 通过 SQL 进行 Join，生成订单宽表（包含 event_type）
 * 4. 结果写入 Kafka (Upsert 模式)
 */
public class DwdOrderDetailWideJob {

    public static void main(String[] args) throws Exception {
        // 1. Flink 配置：设置内存参数
        Configuration conf = new Configuration();
        conf.setFloat("taskmanager.memory.network.fraction", 0.3f);
        conf.setString("taskmanager.memory.network.min", "256mb");
        conf.setString("taskmanager.memory.network.max", "256mb");

        // 2. 创建流执行环境（带 Web UI，方便调试）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 3. 创建 Table API 环境（流模式）
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 4. 注册 Kafka 源表（ODS，Debezium 格式）
        tableEnv.executeSql(
                "CREATE TABLE ods_order (\n" +
                        "  `op` STRING,\n" +   // CDC 操作类型 (c/u/d)
                        "  `after` ROW<        -- 变化后的行数据\n" +
                        "    id STRING,\n" +
                        "    order_id STRING,\n" +
                        "    order_detail_id STRING,\n" +
                        "    sku_id STRING,\n" +
                        "    sku_name STRING,\n" +
                        "    order_price STRING,\n" +
                        "    sku_num STRING,\n" +
                        "    create_time STRING,\n" +
                        "    user_id STRING,\n" +
                        "    province_id STRING,\n" +
                        "    activity_id STRING,\n" +
                        "    activity_rule_id STRING,\n" +
                        "    coupon_id STRING,\n" +
                        "    coupon_use_id STRING,\n" +
                        "    payment_type STRING,\n" +
                        "    callback_time STRING,\n" +
                        "    total_amount STRING,\n" +
                        "    refund_amount STRING,\n" +
                        "    refund_reason_type STRING,\n" +
                        "    refund_reason_txt STRING,\n" +
                        "    refund_create_time STRING\n" +
                        "  >,\n" +
                        "  `source` MAP<STRING, STRING>, -- Debezium 来源表名\n" +
                        "  `ts_ms` BIGINT,               -- CDC 时间戳\n" +
                        "  `proc_time` AS PROCTIME()     -- 处理时间\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'ods_ecommerce_order',\n" +
                        "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                        "  'properties.group.id' = 'flink_order_consumer',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json',\n" +
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")"
        );

        // 5. 拆分表视图：根据 source.table 字段判断是哪张表
        tableEnv.executeSql("CREATE TEMPORARY VIEW order_detail AS SELECT * FROM ods_order WHERE `source`['table'] = 'order_detail'");
        tableEnv.executeSql("CREATE TEMPORARY VIEW order_info AS SELECT * FROM ods_order WHERE `source`['table'] = 'order_info'");
        tableEnv.executeSql("CREATE TEMPORARY VIEW order_detail_coupon AS SELECT * FROM ods_order WHERE `source`['table'] = 'order_detail_coupon'");
        tableEnv.executeSql("CREATE TEMPORARY VIEW order_detail_activity AS SELECT * FROM ods_order WHERE `source`['table'] = 'order_detail_activity'");
        tableEnv.executeSql("CREATE TEMPORARY VIEW payment_info AS SELECT * FROM ods_order WHERE `source`['table'] = 'payment_info'");
        tableEnv.executeSql("CREATE TEMPORARY VIEW order_refund_info AS SELECT * FROM ods_order WHERE `source`['table'] = 'order_refund_info'");

        // 6. 对退款表去重：同一订单取最新一条退款记录
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW refund_info_dedup AS\n" +
                        "SELECT * FROM (\n" +
                        "  SELECT *, ROW_NUMBER() OVER (PARTITION BY after.order_id ORDER BY after.refund_create_time DESC) AS rn\n" +
                        "  FROM order_refund_info\n" +
                        ") t WHERE rn = 1"
        );

        // 7. 创建宽表视图（包含 event_type 字段，标识事件类型：order/payment/refund）
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW dwd_order_detail_wide AS\n" +
                        "SELECT\n" +
                        "  od.after.id                       AS order_detail_id,\n" +
                        "  od.after.order_id                AS order_id,\n" +
                        "  od.after.sku_id                  AS sku_id,\n" +
                        "  od.after.sku_name                AS sku_name,\n" +
                        "  od.after.order_price             AS order_price,\n" +
                        "  od.after.sku_num                 AS sku_num,\n" +
                        "  od.after.create_time             AS order_detail_create_time,\n" +
                        "  oi.after.user_id                 AS user_id,\n" +
                        "  oi.after.province_id             AS province_id,\n" +
                        "  oi.after.create_time             AS order_create_time,\n" +
                        "  act.after.activity_id            AS activity_id,\n" +
                        "  cou.after.coupon_id              AS coupon_id,\n" +
                        "  pay.after.payment_type           AS payment_type,\n" +
                        "  pay.after.callback_time          AS payment_time,\n" +
                        "  pay.after.total_amount           AS pay_amount,\n" +
                        "  refund.after.refund_amount       AS refund_amount,\n" +
                        "  refund.after.refund_reason_type  AS refund_reason_type,\n" +
                        "  refund.after.refund_create_time  AS refund_create_time,\n" +
                        "  CASE\n" +
                        "    WHEN refund.after.refund_amount IS NOT NULL THEN 'refund'\n" +
                        "    WHEN pay.after.total_amount IS NOT NULL THEN 'payment'\n" +
                        "    ELSE 'order'\n" +
                        "  END                              AS event_type,\n" + // ✅ 事件类型
                        "  od.ts_ms                         AS ts,\n" +
                        "  od.proc_time                     AS proc_time\n" +
                        "FROM order_detail od\n" +
                        "LEFT JOIN order_info oi ON od.after.order_id = oi.after.id\n" +
                        "LEFT JOIN order_detail_activity act ON od.after.id = act.after.order_detail_id\n" +
                        "LEFT JOIN order_detail_coupon cou ON od.after.id = cou.after.order_detail_id\n" +
                        "LEFT JOIN payment_info pay ON od.after.order_id = pay.after.order_id\n" +
                        "LEFT JOIN refund_info_dedup refund ON od.after.order_id = refund.after.order_id"
        );

        // 8. 创建 Kafka Sink 表（Upsert 模式，key=order_detail_id）
        tableEnv.executeSql(
                "CREATE TABLE dwd_order_detail_wide_kafka (\n" +
                        "  order_detail_id STRING,\n" +
                        "  order_id STRING,\n" +
                        "  sku_id STRING,\n" +
                        "  sku_name STRING,\n" +
                        "  order_price STRING,\n" +
                        "  sku_num STRING,\n" +
                        "  order_detail_create_time STRING,\n" +
                        "  user_id STRING,\n" +
                        "  province_id STRING,\n" +
                        "  order_create_time STRING,\n" +
                        "  activity_id STRING,\n" +
                        "  coupon_id STRING,\n" +
                        "  payment_type STRING,\n" +
                        "  payment_time STRING,\n" +
                        "  pay_amount STRING,\n" +
                        "  refund_amount STRING,\n" +
                        "  refund_reason_type STRING,\n" +
                        "  refund_create_time STRING,\n" +
                        "  event_type STRING,\n" +   // ✅ 新增字段
                        "  ts BIGINT,\n" +
                        "  proc_time TIMESTAMP(3),\n" +
                        "  PRIMARY KEY (order_detail_id) NOT ENFORCED\n" + // 主键 Upsert
                        ") WITH (\n" +
                        "  'connector' = 'upsert-kafka',\n" +
                        "  'topic' = 'dwd_order_detail_wide_kafka',\n" +
                        "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                        "  'key.format' = 'json',\n" +
                        "  'value.format' = 'json'\n" +
                        ")"
        );

        // 9. 将宽表数据写入 Kafka（过滤掉 user_id 为空的数据）
        tableEnv.executeSql(
                "INSERT INTO dwd_order_detail_wide_kafka\n" +
                        "SELECT * FROM dwd_order_detail_wide\n" +
                        "WHERE user_id IS NOT NULL"
        );
    }
}
