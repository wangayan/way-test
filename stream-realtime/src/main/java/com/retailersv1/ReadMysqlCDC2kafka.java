package com.retailersv1;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Title: ReadMysqlCDC2kafka
 * @Author wangayan
 * @Package com.stream.common.domain
 * @Date 2025/8/15 14:57
 * @description: 读取kafka中的数据
 */
public class ReadMysqlCDC2kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = KafkaUtils.buildKafkaSecureSource(
                ConfigUtils.getString("kafka.bootstrap.servers"),
                "realtime_log",
                "group",
                OffsetsInitializer.earliest()
        );

        DataStream<String> stream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),   // 如果有事件时间，换成实际的水位线策略
                "kafka_source"
        );
        stream.print();
        env.execute();
    }

}
