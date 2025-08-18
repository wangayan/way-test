package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.domain.TableProcessDim;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.HbaseUtils;
import com.stream.common.utils.JdbcUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessSpiltStreamToHBaseDimFunc extends BroadcastProcessFunction<JSONObject,JSONObject,JSONObject> {

    private MapStateDescriptor<String,JSONObject> mapStateDescriptor;
    private HashMap<String, TableProcessDim> configMap =  new HashMap<>();
    private org.apache.hadoop.hbase.client.Connection hbaseConnection;
    private HbaseUtils hbaseUtils;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化MySQL连接
        Connection connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));

        // 加载维度表配置
        String querySQL = "select * from realtime_v1_config.table_process_dim";
        List<TableProcessDim> tableProcessDims = JdbcUtils.queryList(connection, querySQL, TableProcessDim.class, true);

        for (TableProcessDim tableProcessDim : tableProcessDims) {
            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        connection.close();

        // 初始化HBase连接
        hbaseUtils = new HbaseUtils(ConfigUtils.getString("zookeeper.server.host.list"));
        hbaseConnection = hbaseUtils.getConnection();

        // 预创建所有需要的HBase表
        ensureAllTablesExist();
    }

    /**
     * 确保所有需要的HBase表都存在
     */
    private void ensureAllTablesExist() throws Exception {
        try (Admin admin = hbaseConnection.getAdmin()) {
            for (TableProcessDim dimConfig : configMap.values()) {
                String tableName = "default:" + dimConfig.getSinkTable();
                TableName hbaseTableName = TableName.valueOf(tableName);

                if (!admin.tableExists(hbaseTableName)) {
                    // 创建表
                    TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(hbaseTableName);

                    // 添加列族（使用配置中的列族，默认为"info"）
                    String columnFamily = dimConfig.getSinkFamily() != null ?
                            dimConfig.getSinkFamily() : "info";
                    tableBuilder.setColumnFamily(
                            ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily)).build()
                    );

                    admin.createTable(tableBuilder.build());
                    System.out.println("Created HBase table: " + tableName);
                }
            }
        }
    }

    public ProcessSpiltStreamToHBaseDimFunc(MapStateDescriptor<String, JSONObject> mapStageDesc) {
        this.mapStateDescriptor = mapStageDesc;
    }

    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        ReadOnlyBroadcastState<String, JSONObject> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String tableName = jsonObject.getJSONObject("source").getString("table");
        JSONObject broadData = broadcastState.get(tableName);

        // 获取表配置（优先使用广播状态中的配置）
        TableProcessDim tableConfig = null;
        if (broadData != null) {
            tableConfig = broadData.getJSONObject("after").toJavaObject(TableProcessDim.class);
        } else {
            tableConfig = configMap.get(tableName);
        }

        if (tableConfig != null && tableConfig.getSourceTable().equals(tableName)) {
            if (!jsonObject.getString("op").equals("d")) {
                JSONObject after = jsonObject.getJSONObject("after");
                String sinkTableName = "default:" + tableConfig.getSinkTable();

                // 确保表存在（双重检查）
                ensureTableExists(sinkTableName, tableConfig.getSinkFamily());

                String hbaseRowKey = after.getString(tableConfig.getSinkRowKey());
                try (Table hbaseTable = hbaseConnection.getTable(TableName.valueOf(sinkTableName))) {
                    Put put = new Put(Bytes.toBytes(MD5Hash.getMD5AsHex(hbaseRowKey.getBytes(StandardCharsets.UTF_8))));

                    // 使用配置中的列族，默认为"info"
                    String columnFamily = tableConfig.getSinkFamily() != null ?
                            tableConfig.getSinkFamily() : "info";

                    for (Map.Entry<String, Object> entry : after.entrySet()) {
                        put.addColumn(
                                Bytes.toBytes(columnFamily),
                                Bytes.toBytes(entry.getKey()),
                                Bytes.toBytes(String.valueOf(entry.getValue()))
                        );
                    }

                    hbaseTable.put(put);
                    System.err.println("Successfully put to HBase: " + sinkTableName +
                            ", rowKey: " + Arrays.toString(put.getRow()));
                }
            }
        }
    }

    /**
     * 确保单个表存在
     */
    private void ensureTableExists(String tableName, String columnFamily) throws Exception {
        try (Admin admin = hbaseConnection.getAdmin()) {
            TableName hbaseTableName = TableName.valueOf(tableName);
            if (!admin.tableExists(hbaseTableName)) {
                TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(hbaseTableName);

                // 使用传入的列族，如果为空则使用默认值"info"
                String cf = columnFamily != null ? columnFamily : "info";
                tableBuilder.setColumnFamily(
                        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build()
                );

                admin.createTable(tableBuilder.build());
                System.out.println("Created HBase table on demand: " + tableName);
            }
        }
    }

    @Override
    public void processBroadcastElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
        BroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String op = jsonObject.getString("op");

        if (jsonObject.containsKey("after")) {
            JSONObject after = jsonObject.getJSONObject("after");
            String sourceTableName = after.getString("source_table");

            if ("d".equals(op)) {
                broadcastState.remove(sourceTableName);
            } else {
                broadcastState.put(sourceTableName, jsonObject);

                // 当有新配置时，确保对应的HBase表存在
                String sinkTable = after.getString("sink_table");
                String columnFamily = after.getString("sink_family");
                ensureTableExists("default:" + sinkTable, columnFamily);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (hbaseConnection != null) {
            hbaseConnection.close();
        }
        super.close();
    }
}