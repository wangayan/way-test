package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.MapUpdateHbaseDimTableFunc;
import com.retailersv1.func.ProcessSpiltStreamToHBaseDimFunc;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * @Title: UserTopicDwdHBase
 * @Author wang.Ayan
 * @Date 2025/8/20 8:50
 * @Package com.retailers
 * @description:
 */
public class UserTopicDwdHBase {

    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String CDH_KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-chk");

        // ==================== 1. 配置库 CDC Source（table_process_dwd） ====================
        MySqlSource<String> mySQLCdcDimConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "realtime_v1_config.table_process_dwd",   //
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial(),
                "5501-5600"
        );

        DataStreamSource<String> cdcDbDimStream = env.fromSource(
                mySQLCdcDimConfSource,
                WatermarkStrategy.noWatermarks(),
                "mysql_cdc_dim_source"
        ).setParallelism(1);

        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMap = cdcDbDimStream.map(JSONObject::parseObject)
                .uid("dim_data_convert_json")
                .name("dim_data_convert_json")
                .setParallelism(1);

        // 清理字段 + 只保留 dim_ 开头的表
        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMapCleanColumn = cdcDbDimStreamMap
                .map(s -> {
                    s.remove("source");
                    s.remove("transaction");
                    JSONObject resJson = new JSONObject();
                    if ("d".equals(s.getString("op"))) {
                        resJson.put("before", s.getJSONObject("before"));
                    } else {
                        resJson.put("after", s.getJSONObject("after"));
                    }
                    resJson.put("op", s.getString("op"));
                    return resJson;
                })
                .filter(json -> {
                    JSONObject after = json.getJSONObject("after");
                    JSONObject before = json.getJSONObject("before");
                    String tableName = null;
                    if (after != null) {
                        tableName = after.getString("sink_table");
                    } else if (before != null) {
                        tableName = before.getString("sink_table");
                    }
                    return tableName != null && tableName.startsWith("dim_"); //
                })
                .uid("clean_json_column_map")
                .name("clean_json_column_map");

        // 将配置表同步到 HBase，动态创建/更新维度表
        SingleOutputStreamOperator<JSONObject> tpDS = cdcDbDimStreamMapCleanColumn
                .map(new MapUpdateHbaseDimTableFunc(CDH_ZOOKEEPER_SERVER, CDH_HBASE_NAME_SPACE))
                .uid("map_create_hbase_dim_table")
                .name("map_create_hbase_dim_table");

        // 广播配置流
        MapStateDescriptor<String, JSONObject> mapStageDesc =
                new MapStateDescriptor<>("mapStageDesc", String.class, JSONObject.class);
        BroadcastStream<JSONObject> broadcastDs = tpDS.broadcast(mapStageDesc);

        // ==================== 2. 事实流来源：Kafka DWD ====================
        DataStreamSource<String> kafkaDwdStream = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        CDH_KAFKA_SERVER,
                        "dwd_user_behavior_log",
                        "dwd_user_behavior_log_group",
                        OffsetsInitializer.earliest() //
                ),
                WatermarkStrategy.noWatermarks(),
                "kafka_dwd_user_behavior_log_source"
        ).setParallelism(1);

        SingleOutputStreamOperator<JSONObject> dwdStreamMap = kafkaDwdStream
                .map(JSONObject::parseObject)
                .uid("dwd_data_convert_json")
                .name("dwd_data_convert_json")
                .setParallelism(1);

        dwdStreamMap.print("dwdStreamMap -> ");

        // ==================== 3. 事实流 + 配置流 联合处理 ====================
        BroadcastConnectedStream<JSONObject, JSONObject> connectDs = dwdStreamMap.connect(broadcastDs);

        connectDs.process(new ProcessSpiltStreamToHBaseDimFunc(mapStageDesc))
                .uid("process_split_stream_to_hbase")
                .name("process_split_stream_to_hbase");

        env.disableOperatorChaining();
        env.execute();
    }
}
