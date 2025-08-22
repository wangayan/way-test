package com.retailersv.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwdErrorLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 从 Kafka 读取 ods_log 源数据
        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaUtils.getKafkaSource("realtime_log", "realtime_log_group", "cdh01:9092"),
                WatermarkStrategy.noWatermarks(),
                "Kafka_Source"
        );

        // 3. 解析 JSON 并过滤包含 "err" 的日志
        SingleOutputStreamOperator<String> errorLogStream = kafkaSource
                .map(JSONObject::parseObject)
                .filter(json -> json.containsKey("err"))
                .map(JSON::toJSONString)
                .name("Error_Log_Filter");

        // 4. 写入 Kafka DWD 错误日志主题
        errorLogStream.sinkTo(
                KafkaUtils.buildKafkaSink("cdh01:9092", "dwd_error_log")
        );




        env.execute();
    }
}
