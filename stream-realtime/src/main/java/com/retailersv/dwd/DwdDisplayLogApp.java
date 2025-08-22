package com.retailersv.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class DwdDisplayLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 从 Kafka 中读取 ods_page_log
        KafkaSource<String> kafkaSource = KafkaUtils.getKafkaSource(
                "realtime_log",
                "realtime_log_group",
                "cdh01:9092"
        );

        DataStreamSource<String> kafkaStrStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka_ODS_Page_Log"
        );

        // 3. 转换为 JSON 并过滤非法数据
        SingleOutputStreamOperator<JSONObject> jsonStream = kafkaStrStream
                .map(JSON::parseObject)
                .filter(json -> json.containsKey("displays"))
                .uid("parse_and_filter_json")
                .name("Filter_Has_Displays");

        // 4. 拆分 displays 数组为一条条记录
        SingleOutputStreamOperator<String> displayLogStream = jsonStream
                .flatMap((FlatMapFunction<JSONObject, String>) (json, out) -> {
                    JSONObject common = json.getJSONObject("common");
                    JSONObject page = json.getJSONObject("page");
                    Long ts = json.getLong("ts");

                    JSONArray displays = json.getJSONArray("displays");
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject displayObj = displays.getJSONObject(i);

                        // 拼接结构：保留 common/page/ts 结构
                        JSONObject result = new JSONObject();
                        result.put("common", common);
                        result.put("page", page);
                        result.put("ts", ts);
                        result.put("display", displayObj);

                        out.collect(result.toJSONString());
                    }
                })
                .returns(Types.STRING)
                .name("FlatMap_Displays");

        // 5. 写入 Kafka 的 DWD 层 topic：dwd_traffic_display_log
        displayLogStream
                .sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "dwd_traffic_display_log"))
                .name("Sink_To_DWD_Display_Log");






        env.execute();
    }
}
