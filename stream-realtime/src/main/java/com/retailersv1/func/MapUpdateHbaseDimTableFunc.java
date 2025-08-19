package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.HbaseUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MapUpdateHbaseDimTableFunc extends RichMapFunction<JSONObject, JSONObject> {
    private static final Logger LOG = LoggerFactory.getLogger(MapUpdateHbaseDimTableFunc.class);

    private Connection connection;
    private final String hbaseNameSpace;
    private final String zkHostList;
    private HbaseUtils hbaseUtils;

    public MapUpdateHbaseDimTableFunc(String cdhZookeeperServer, String cdhHbaseNameSpace) {
        this.zkHostList = cdhZookeeperServer;
        this.hbaseNameSpace = cdhHbaseNameSpace;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        hbaseUtils = new HbaseUtils(zkHostList);
        connection = hbaseUtils.getConnection();

        // 添加连接验证
        if (connection == null || connection.isClosed()) {
            throw new RuntimeException("HBase connection initialization failed");
        }

        // 测试连接可用性
        try (Admin admin = connection.getAdmin()) {
            LOG.info("HBase connection validated. Cluster status: {}", admin.getClusterStatus());
        }
    }

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        String op = jsonObject.getString("op");
        if (op == null) {
            LOG.warn("Missing 'op' field in record: {}", jsonObject);
            return jsonObject;
        }

        try {
            switch (op) {
                case "d":
                    handleDeleteOperation(jsonObject);
                    break;
                case "r":
                case "c":
                    handleCreateOperation(jsonObject);
                    break;
                default:
                    handleDefaultOperation(jsonObject);
            }
        } catch (Exception e) {
            LOG.error("Error processing record: {}", jsonObject, e);
            // 根据业务需求决定是否抛出异常
            // throw e;
        }

        return jsonObject;
    }

    private void handleDeleteOperation(JSONObject json) {
        JSONObject before = json.getJSONObject("before");
        if (before == null) {
            LOG.warn("'before' field missing for delete operation");
            return;
        }

        String tableName = before.getString("sink_table");
        if (tableName == null) {
            LOG.warn("'sink_table' missing in 'before' field");
            return;
        }

        String fullTableName = hbaseNameSpace + ":" + tableName;
        try {
            if (hbaseUtils.tableIsExists(fullTableName)) {
                LOG.info("Deleting table: {}", fullTableName);
                hbaseUtils.deleteTable(fullTableName);
            } else {
                LOG.warn("Table {} does not exist, skip delete", fullTableName);
            }
        } catch (Exception e) {
            LOG.error("Failed to delete table: {}", fullTableName, e);
        }
    }

    private void handleCreateOperation(JSONObject json) {
        JSONObject after = json.getJSONObject("after");
        if (after == null) {
            LOG.warn("'after' field missing for create operation");
            return;
        }

        String tableName = after.getString("sink_table");
        if (tableName == null) {
            LOG.warn("'sink_table' missing in 'after' field");
            return;
        }

        String fullTableName = hbaseNameSpace + ":" + tableName;
        try {
            if (!hbaseUtils.tableIsExists(fullTableName)) {
                LOG.info("Creating table: {}", fullTableName);
                hbaseUtils.createTable(hbaseNameSpace, tableName);
            } else {
                LOG.info("Table {} already exists", fullTableName);
            }
        } catch (Exception e) {
            LOG.error("Failed to create table: {}", fullTableName, e);
        }
    }

    private void handleDefaultOperation(JSONObject json) {
        // 先处理删除
        handleDeleteOperation(json);

        // 然后处理创建
        handleCreateOperation(json);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            try {
                connection.close();
                LOG.info("HBase connection closed");
            } catch (IOException e) {
                LOG.error("Error closing HBase connection", e);
            }
        }
    }
}