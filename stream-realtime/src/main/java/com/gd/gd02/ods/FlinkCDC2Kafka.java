package com.gd.gd02.ods;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import java.util.Properties;

/**
 * @Title: FlinkCDC2Kafka
 * @Author wang.Ayan
 * @Date 2025/8/27 10:08
 * @Package com.gd.gd02.ods
 * @description: flinkCDC读取mysql中的数据，写到kafka
 */
public class FlinkCDC2Kafka {

    public static void main(String[] args) throws Exception {
        // 1. Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 配置 MySQL CDC Source
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("cdh01")              // MySQL 地址
                .port(3306)                         // MySQL 端口
                .databaseList("gd02")          // 订阅的库
                .tableList("gd02.consult_log") // 订阅的表
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 输出 JSON 格式
                .startupOptions(StartupOptions.initial()) // 第一次跑先全量，再实时增量
                .build();

        // 3. 获取 CDC 数据流
        DataStreamSource<String> mysqlStream = env.fromSource(
                mysqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Source"
        );

        // 4. 配置 Kafka Producer
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "cdh01:9092"); // ⚠️ 修改为你的 Kafka 地址
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(
                "ods_consult",                       // Kafka 主题
                new org.apache.flink.api.common.serialization.SimpleStringSchema(),
                props
        );

        // 5. 写入 Kafka
        mysqlStream.addSink(kafkaSink);

        // 6. 启动作业
        env.execute("ODS Consult Log CDC → Kafka");
    }
}

