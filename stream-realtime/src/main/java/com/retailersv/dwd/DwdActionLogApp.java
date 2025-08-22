package com.retailersv.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class DwdActionLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 获取 Kafka 源（ODS 页面日志）
        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaUtils.getKafkaSource("realtime_log", "realtime_log_group", "cdh01:9092"),
                WatermarkStrategy.noWatermarks(),
                "ODS_Page_Source"
        );

        // 3. 转换为 JSON
        SingleOutputStreamOperator<JSONObject> jsonStream = kafkaSource
                .map(JSONObject::parseObject)
                .name("Parse_JSON");

        // 4. 拆解动作日志（actions 是数组）
        SingleOutputStreamOperator<String> actionLogStream = jsonStream
                .flatMap((FlatMapFunction<JSONObject, String>) (json, out) -> {
                    JSONArray actions = json.getJSONArray("actions");
                    if (actions != null) {
                        JSONObject common = json.getJSONObject("common");
                        JSONObject page = json.getJSONObject("page");
                        Long ts = json.getLong("ts");

                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject result = new JSONObject();
                            result.put("common", common);
                            result.put("page", page);
                            result.put("action", actions.getJSONObject(i));
                            result.put("ts", ts);
                            out.collect(result.toJSONString());
                        }
                    }
                })
                .returns(Types.STRING)
                .name("FlatMap_Actions");

        // 5. 写入 Kafka 的 DWD 层
        actionLogStream.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "dwd_action_log"));









        env.execute();
    }
}
