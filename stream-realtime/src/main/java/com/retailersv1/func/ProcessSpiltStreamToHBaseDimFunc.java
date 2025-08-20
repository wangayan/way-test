package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessSpiltStreamToHBaseDimFunc extends BroadcastProcessFunction<JSONObject, JSONObject, JSONObject> {

    private final MapStateDescriptor<String, JSONObject> mapStageDesc;

    public ProcessSpiltStreamToHBaseDimFunc(MapStateDescriptor<String, JSONObject> mapStageDesc) {
        this.mapStageDesc = mapStageDesc;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void processElement(JSONObject value,
                               ReadOnlyContext ctx,
                               Collector<JSONObject> out) throws Exception {
        if (value == null || value.isEmpty()) {
            return;
        }

        // 默认当作 CDC 格式处理
        JSONObject data = value.getJSONObject("after");
        String table = value.getString("table");

        // 如果是 DWD 埋点日志（没有 after 字段）
        if (data == null || table == null) {
            data = value;
            table = "dwd_user_behavior_log";  // 给埋点流一个固定表名
        }

        // 获取配置
        ReadOnlyBroadcastState<String, JSONObject> configState = ctx.getBroadcastState(mapStageDesc);
        JSONObject config = configState.get(table);

        if (config == null) {
            System.out.println("No config for table: " + table);
            return;
        }

        // 打上配置里的 sink 信息（比如 hbase 表名等）
        data.put("sink_table", config.getString("sink_table"));

        // 输出
        out.collect(data);
    }

    @Override
    public void processBroadcastElement(JSONObject value,
                                        Context ctx,
                                        Collector<JSONObject> out) throws Exception {
        // 更新配置表
        if (value == null || value.isEmpty()) {
            return;
        }

        String sourceTable = value.getString("source_table");
        if (sourceTable == null) {
            return;
        }

        ctx.getBroadcastState(mapStageDesc).put(sourceTable, value);
    }
}
