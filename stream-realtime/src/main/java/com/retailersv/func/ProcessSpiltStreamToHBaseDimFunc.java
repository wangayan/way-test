package com.retailersv.func;

import com.alibaba.fastjson.JSONObject;
import com.retailersv.domain.TableProcessDim;
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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ProcessSpiltStreamToHBaseDimFunc extends BroadcastProcessFunction<JSONObject,JSONObject,JSONObject> {

    private MapStateDescriptor<String,JSONObject> mapStateDescriptor;
    private HashMap<String, TableProcessDim> configMap =  new HashMap<>();

    private org.apache.hadoop.hbase.client.Connection hbaseConnection ;

    private HbaseUtils hbaseUtils;



    @Override
    public void open(Configuration parameters) throws Exception {
        Connection connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));
        String querySQL = "select * from realtime_v1_config.table_process_dim";
        List<TableProcessDim> tableProcessDims = JdbcUtils.queryList(connection, querySQL, TableProcessDim.class, true);
        // configMap:spu_info -> TableProcessDim(sourceTable=spu_info, sinkTable=dim_spu_info, sinkColumns=id,spu_name,description,category3_id,tm_id, sinkFamily=info, sinkRowKey=id, op=null)
        for (TableProcessDim tableProcessDim : tableProcessDims ){
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        connection.close();
        hbaseUtils = new HbaseUtils(ConfigUtils.getString("zookeeper.server.host.list"));
        hbaseConnection = hbaseUtils.getConnection();
    }

    public ProcessSpiltStreamToHBaseDimFunc(MapStateDescriptor<String, JSONObject> mapStageDesc) {
        this.mapStateDescriptor = mapStageDesc;
    }

    @Override
    public void processElement(JSONObject jsonObject,
                               BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext readOnlyContext,
                               Collector<JSONObject> collector) throws Exception {

        ReadOnlyBroadcastState<String, JSONObject> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String tableName = jsonObject.getJSONObject("source").getString("table");
        JSONObject broadData = broadcastState.get(tableName);

        if (broadData != null || configMap.get(tableName) != null) {
            if (configMap.get(tableName).getSourceTable().equals(tableName)) {

                if (!jsonObject.getString("op").equals("d")) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    String sinkTableName = configMap.get(tableName).getSinkTable();
                    sinkTableName = "default:" + sinkTableName;

                    // 获取原始字段作为 rowKey
                    String hbaseRowKey = after.getString(configMap.get(tableName).getSinkRowKey());

                    // ❌ 注释掉原始 MD5 逻辑
                    // Put put = new Put(Bytes.toBytes(MD5Hash.getMD5AsHex(hbaseRowKey.getBytes(StandardCharsets.UTF_8))));

                    // ✅ 改为使用明文 rowKey（可选加盐）
                    // int salt = Math.abs(hbaseRowKey.hashCode()) % 10;
                    // String saltedRowKey = salt + "_" + hbaseRowKey;
                    // Put put = new Put(Bytes.toBytes(saltedRowKey));

                    Put put = new Put(Bytes.toBytes(hbaseRowKey));  // 直接使用明文 rowKey

                    for (Map.Entry<String, Object> entry : after.entrySet()) {
                        put.addColumn(Bytes.toBytes("info"),
                                Bytes.toBytes(entry.getKey()),
                                Bytes.toBytes(String.valueOf(entry.getValue())));
                    }

                    Table hbaseConnectionTable = hbaseConnection.getTable(TableName.valueOf(sinkTableName));
                    hbaseConnectionTable.put(put);

                    System.err.println("put -> " + put.toJSON() + " rowKey=" + hbaseRowKey);
                }
            }
        }
    }


    @Override
    public void processBroadcastElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        // {"op":"r","after":{"sink_row_key":"id","sink_family":"info","sink_table":"dim_base_category2","source_table":"base_category2","sink_columns":"id,name,category1_id"}}
//        System.err.println("processBroadcastElement jsonObject -> "+ jsonObject.toString());
        BroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(mapStateDescriptor);
        // HeapBroadcastState{stateMetaInfo=RegisteredBroadcastBackendStateMetaInfo{name='mapStageDesc', keySerializer=org.apache.flink.api.common.typeutils.base.StringSerializer@39529185, valueSerializer=org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer@59b0797e, assignmentMode=BROADCAST}, backingMap={}, internalMapCopySerializer=org.apache.flink.api.common.typeutils.base.MapSerializer@4ab01899}
        String op = jsonObject.getString("op");
        if (jsonObject.containsKey("after")){
            String sourceTableName = jsonObject.getJSONObject("after").getString("source_table");
            if ("d".equals(op)){
                broadcastState.remove(sourceTableName);
            }else {
                broadcastState.put(sourceTableName,jsonObject);
//                configMap.put(sourceTableName,jsonObject.toJavaObject(TableProcessDim.class));
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        hbaseConnection.close();
    }
}
