package com.retailersv.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 日志 ODS 层采集程序：
 * 1. 从 Kafka 日志主题消费原始日志
 * 2. 按日志类型拆分：启动 / 页面 / 曝光 / 动作 / 错误
 * 3. 写入 Kafka 对应 ODS Topic
 */
public class OdsLogBaseStreamApp {
    public static void main(String[] args) throws Exception {

        // 1. 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置并行度为 1，保证调试时输出有序

        // 2. 定义 Kafka Source，用于读取实时日志数据
        KafkaSource<String> source = KafkaUtils.getKafkaSource(
                "realtime_log",   // 输入日志主题
                "ods_log_group",  // 消费者组
                "cdh01:9092"      // Kafka Broker 地址
        );

        // 3. 从 Kafka 读取数据，并将 String 转换为 JSONObject
        SingleOutputStreamOperator<JSONObject> jsonStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka_Source") // 不生成水位线
                .map(JSON::parseObject)   // 将字符串解析成 JSON 对象
                .name("Parse_JSON");

        // 4. 拆分日志流
        // 4.1 启动日志（包含 start 字段）
        SingleOutputStreamOperator<String> startLogStream = jsonStream
                .filter(json -> json.containsKey("start")) // 判断是否有 "start" 字段
                .map(JSON::toJSONString)                   // 转回 String 写入 Kafka
                .name("Start_Log");

        // 4.2 页面日志（包含 page 字段）
        SingleOutputStreamOperator<String> pageLogStream = jsonStream
                .filter(json -> json.containsKey("page"))
                .map(JSON::toJSONString)
                .name("Page_Log");

        // 4.3 曝光日志（包含 displays 数组字段）
        SingleOutputStreamOperator<String> displayLogStream = jsonStream
                .filter(json -> json.containsKey("displays"))
                .map(JSON::toJSONString)
                .name("Display_Log");

        // 4.4 动作日志（包含 actions 数组字段）
        SingleOutputStreamOperator<String> actionLogStream = jsonStream
                .filter(json -> json.containsKey("actions"))
                .map(JSON::toJSONString)
                .name("Action_Log");

        // 4.5 错误日志（包含 err 字段）
        SingleOutputStreamOperator<String> errorLogStream = jsonStream
                .filter(json -> json.containsKey("err"))
                .map(JSON::toJSONString)
                .name("Error_Log");

        // 5. 将不同类型日志写入 Kafka 对应 ODS Topic
        startLogStream.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "realtime_start_log"));
        pageLogStream.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "realtime_page_log"));
        displayLogStream.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "realtime_display_log"));
        actionLogStream.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "realtime_action_log"));
        errorLogStream.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "realtime_err_log"));

        // 6. 提交执行
        env.execute();
    }
}
