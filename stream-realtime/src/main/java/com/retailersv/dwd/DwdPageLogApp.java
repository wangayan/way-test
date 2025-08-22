package com.retailersv.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwdPageLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从 Kafka 读取页面日志
        KafkaSource<String> kafkaSource = KafkaUtils.getKafkaSource(
                "realtime_log",       // 源 Kafka 主题
                "realtime_log_group", // groupId
                "cdh01:9092"          // broker
        );
        DataStreamSource<String> kafkaStrStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka_ODS_Page_Log"
        );

        kafkaStrStream.print();

        //  转换为 JSON 并过滤非法数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = kafkaStrStream
                .map(JSON::parseObject)
                .filter(json -> json != null && json.containsKey("common") && json.containsKey("page"))
                .uid("parse_and_filter_json")
                .name("Parse_And_Filter_JSON");

        //输出到 Kafka DWD 层：dwd_traffic_page_log
        jsonObjStream
                .map(JSON::toJSONString)
                .sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "dwd_traffic_page_log"))
                .uid("sink_to_dwd_traffic_page_log")
                .name("Sink_To_DWD_Page_Log");


        env.execute();
    }
}
