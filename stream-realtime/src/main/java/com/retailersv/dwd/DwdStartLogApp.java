package com.retailersv.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwdStartLogApp {
    public static void main(String[] args) throws Exception {

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取 Kafka 源数据
        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaUtils.getKafkaSource("realtime_log", "realtime_log_group", "cdh01:9092"),
                WatermarkStrategy.noWatermarks(),
                "Kafka_Source"
        );

        // 3. 过滤包含 "start" 字段的日志
        SingleOutputStreamOperator<String> startLogStream = kafkaSource
                .map(JSONObject::parseObject)
                .filter(json -> json.containsKey("start"))
                .map(JSON::toJSONString)
                .name("Start_Log_Filter");

        // 4. 写入 Kafka dwd_start_log 主题
        startLogStream.sinkTo(
                KafkaUtils.buildKafkaSink("cdh01:9092", "dwd_start_log")
        );

        // 5. 执行任务
        env.execute();
    }
}
