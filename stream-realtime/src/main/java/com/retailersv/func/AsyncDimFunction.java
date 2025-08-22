package com.retailersv.func;

import avro.shaded.com.google.common.cache.Cache;
import avro.shaded.com.google.common.cache.CacheBuilder;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.HbaseUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 通用异步维度拉取函数基类（支持 HBase 查询 + Guava 缓存 + JSONObject 补充）
 * @param <T> 输入流类型（可以是 JSONObject 或实体类）
 */
public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T, T> {

    private final String tableName;
    private transient Connection conn;
    private transient Table table;
    private transient Cache<String, JSONObject> cache;

    public AsyncDimFunction(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 抽象方法：子类指定如何提取维度 RowKey
     */
    public abstract String getRowKey(T input);

    /**
     * 抽象方法：子类指定如何将维度信息补充到 input 中
     */
    public abstract void join(T input, JSONObject dimInfo);

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = HbaseUtils.getConnection(); // 单例工具类，避免反复连接
        table = conn.getTable(TableName.valueOf(tableName));
        cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {
        String rowKey = getRowKey(input);

        // 1. 查询缓存
        JSONObject dimCache = cache.getIfPresent(rowKey);
        if (dimCache != null) {
            join(input, dimCache);
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        // 2. 异步查 HBase
        CompletableFuture
                .supplyAsync(() -> queryDimFromHBase(rowKey))
                .thenAccept(dimJson -> {
                    if (dimJson != null) {
                        cache.put(rowKey, dimJson);
                        join(input, dimJson);
                    } else {
                        join(input, new JSONObject()); // 空维度占位
                    }
                    resultFuture.complete(Collections.singleton(input));
                });
    }

    /**
     * 从 HBase 查询维度信息，并封装为 JSONObject
     */
    private JSONObject queryDimFromHBase(String rowKey) {
        try {
            Result result = table.get(new Get(Bytes.toBytes(rowKey)));
            if (result == null || result.isEmpty()) return null;

            JSONObject dimJson = new JSONObject();
            result.listCells().forEach(cell -> {
                String col = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                dimJson.put(col, val);
            });
            return dimJson;
        } catch (IOException e) {
            throw new RuntimeException("HBase 查询失败：table=" + tableName + ", rowKey=" + rowKey, e);
        }
    }

    @Override
    public void close() throws Exception {
        if (table != null) table.close();
        if (conn != null) conn.close();
    }
}
