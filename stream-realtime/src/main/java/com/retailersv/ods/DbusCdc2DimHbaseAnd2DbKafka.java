package com.retailersv.ods;

import com.alibaba.fastjson.JSONObject;
import com.retailersv.func.MapUpdateHbaseDimTableFunc;
import com.retailersv.func.ProcessSpiltStreamToHBaseDimFunc;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 该类用于：
 * 1. 从 MySQL 业务库读取 CDC 数据并写入 Kafka
 * 2. 从 MySQL 配置库读取维度配置信息，并动态维护 HBase 维度表
 * 3. 将业务数据流与配置流进行广播连接，实现实时维度关联
 */
public class DbusCdc2DimHbaseAnd2DbKafka {

    // 从配置文件中获取 Zookeeper 地址
    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    // Kafka 集群地址
    private static final String CDH_KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    // HBase 命名空间
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    // Kafka 中 CDC 数据存放的 topic
    private static final String MYSQL_CDC_TO_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");

    @SneakyThrows
    public static void main(String[] args) {
        // 设置 Hadoop 用户，避免写入 HDFS 时权限不足
        System.setProperty("HADOOP_USER_NAME","root");

        // 获取 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置通用参数（并行度、状态后端、重启策略等）
        EnvironmentSettingUtils.defaultParameter(env);

        // 开启 Checkpoint，每 10 秒一次
        env.enableCheckpointing(10000L);
        // 设置 Checkpoint 存储路径（本地测试路径）
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-chk");


        // ============= 1. 定义 MySQL CDC 源：业务数据库 =============
        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),   // 业务数据库
                "",                                        // 表名（为空表示全部）
                ConfigUtils.getString("mysql.user"),       // 用户名
                ConfigUtils.getString("mysql.pwd"),        // 密码
                StartupOptions.initial(),                  // 初始启动方式：从全量+增量开始
                "20000-20050"                              // 分配 source 并行任务 ID 范围
        );

        // ============= 2. 定义 MySQL CDC 源：配置数据库（维度表配置） =============
        MySqlSource<String> mySQLCdcDimConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"), // 配置库
                "gd02.table_process_dim",        // 配置表
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial(),
                "10000-10050"
        );

        // 读取业务数据库 CDC 数据
        DataStreamSource<String> cdcDbMainStream = env.fromSource(
                mySQLDbMainCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_main_source");

        // 读取配置表 CDC 数据
        DataStreamSource<String> cdcDbDimStream = env.fromSource(
                mySQLCdcDimConfSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_dim_source");

        // ============= 3. 业务数据流：JSON 解析 + 写入 Kafka =============
        SingleOutputStreamOperator<JSONObject> cdcDbMainStreamMap = cdcDbMainStream
                .map(JSONObject::parseObject)  // String 转 JSONObject
                .uid("db_data_convert_json")
                .name("_db_data_convert_json")
                .setParallelism(1);

        // 将业务数据写入 Kafka（原始 JSON）
        cdcDbMainStreamMap.map(JSONObject::toString)
                .sinkTo(KafkaUtils.buildKafkaSink(CDH_KAFKA_SERVER, MYSQL_CDC_TO_KAFKA_TOPIC))
                .uid("mysql_cdc_to_kafka_topic")
                .name("_mysql_cdc_to_kafka_topic");

        // 控制台打印业务数据（调试用）
        cdcDbMainStreamMap.print("cdcDbMainStreamMap ->");

        // ============= 4. 配置数据流：JSON 解析 + 清洗 =============
        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMap = cdcDbDimStream
                .map(JSONObject::parseObject)
                .uid("dim_data_convert_json")
                .name("_dim_data_convert_json")
                .setParallelism(1);

        // 清洗配置数据，保留必要字段（before/after/op）
        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMapCleanColumn = cdcDbDimStreamMap.map(s -> {
                    s.remove("source");
                    s.remove("transaction");
                    JSONObject resJson = new JSONObject();
                    if ("d".equals(s.getString("op"))) {   // 删除操作
                        resJson.put("before", s.getJSONObject("before"));
                    } else {                               // 插入/更新操作
                        resJson.put("after", s.getJSONObject("after"));
                    }
                    resJson.put("op", s.getString("op"));  // 操作类型
                    return resJson;
                }).uid("clean_json_column_map")
                .name("_clean_json_column_map");

        // ============= 5. 更新 HBase 维度表结构 =============
        SingleOutputStreamOperator<JSONObject> tpDs = cdcDbDimStreamMapCleanColumn.map(
                        new MapUpdateHbaseDimTableFunc(CDH_ZOOKEEPER_SERVER, CDH_HBASE_NAME_SPACE))
                .uid("map_create_hbase_dim_table")
                .name("map_create_hbase_dim_table");

        // 定义广播状态描述符（存放维度配置）
        MapStateDescriptor<String, JSONObject> mapStageDesc =
                new MapStateDescriptor<>("mapStageDesc", String.class, JSONObject.class);

        // 将维度配置流广播出去
        BroadcastStream<JSONObject> broadcastDs = tpDs.broadcast(mapStageDesc);

        // 将业务数据流与广播配置流连接
        BroadcastConnectedStream<JSONObject, JSONObject> connectDs =
                cdcDbMainStreamMap.connect(broadcastDs);

        // 处理连接流：根据配置动态分流到 HBase 维度表
        connectDs.process(new ProcessSpiltStreamToHBaseDimFunc(mapStageDesc));

        // 禁止算子链优化，方便调试和运维
        env.disableOperatorChaining();

        // 执行 Flink 作业
        env.execute();
    }
}
