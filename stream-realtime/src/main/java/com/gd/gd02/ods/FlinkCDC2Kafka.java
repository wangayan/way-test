package com.gd.gd02.ods;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.json.JSONObject;

import java.util.Properties;

/**
 * @Title: FlinkCDC2Kafka
 * @Author wang.Ayan
 * @Date 2025/8/27 10:08
 * @Package com.gd.gd02.ods
 * @description: flinkCDC读取mysql中的数据，写到kafka
 * 工单编号：大数据-用户画像-02-服务主题店铺绩效
 */
public class FlinkCDC2Kafka {

    // 定义输出标签用于侧输出流
    private static final OutputTag<String> CONSULT_LOG_TAG = new OutputTag<String>("consult_log"){};
    private static final OutputTag<String> ORDER_INFO_TAG = new OutputTag<String>("order_info"){};
    private static final OutputTag<String> PAYMENT_INFO_TAG = new OutputTag<String>("payment_info"){};
    private static final OutputTag<String> CONSULT_ORDER_LINK_TAG = new OutputTag<String>("consult_order_link"){};
    private static final OutputTag<String> PRODUCT_INFO_TAG = new OutputTag<String>("product_info"){};
    private static final OutputTag<String> SHOP_INFO_TAG = new OutputTag<String>("shop_info"){};
    private static final OutputTag<String> CS_INFO_TAG = new OutputTag<String>("cs_info"){};

    public static void main(String[] args) throws Exception {
        // 1. Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 配置 MySQL CDC Source - 监控所有相关表
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("cdh01")
                .port(3306)
                .databaseList("gd02")
                .tableList("gd02.consult_log", "gd02.order_info", "gd02.payment_info",
                        "gd02.consult_order_link", "gd02.product_info", "gd02.shop_info", "gd02.cs_info")
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 输出 JSON 格式
                .startupOptions(StartupOptions.initial()) // 第一次跑先全量，再实时增量
                .build();

        // 3. 获取 CDC 数据流
        DataStream<String> mysqlStream = env.fromSource(
                mysqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Source"
        );

        // 4. 处理数据流，根据表名分流 - 使用 SingleOutputStreamOperator 而不是 DataStream
        SingleOutputStreamOperator<String> processedStream = mysqlStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                try {
                    JSONObject json = new JSONObject(value);
                    JSONObject source = json.getJSONObject("source");
                    String table = source.getString("table");

                    // 添加处理时间戳
                    json.put("process_time", System.currentTimeMillis());

                    // 根据表名分发到不同的侧输出流
                    switch (table) {
                        case "consult_log":
                            ctx.output(CONSULT_LOG_TAG, json.toString());
                            break;
                        case "order_info":
                            ctx.output(ORDER_INFO_TAG, json.toString());
                            break;
                        case "payment_info":
                            ctx.output(PAYMENT_INFO_TAG, json.toString());
                            break;
                        case "consult_order_link":
                            ctx.output(CONSULT_ORDER_LINK_TAG, json.toString());
                            break;
                        case "product_info":
                            ctx.output(PRODUCT_INFO_TAG, json.toString());
                            break;
                        case "shop_info":
                            ctx.output(SHOP_INFO_TAG, json.toString());
                            break;
                        case "cs_info":
                            ctx.output(CS_INFO_TAG, json.toString());
                            break;
                        default:
                            out.collect(json.toString()); // 未知表输出到主流
                    }
                } catch (Exception e) {
                    // 异常处理，输出到主流
                    out.collect(value);
                }
            }
        });

        // 5. 配置 Kafka Producer 属性
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "cdh01:9092,cdh02:9092,cdh03:9092");

        // 6. 为每个表创建Kafka Sink
        FlinkKafkaProducer<String> consultLogSink = new FlinkKafkaProducer<>(
                "ods_consult_log",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> orderInfoSink = new FlinkKafkaProducer<>(
                "ods_order_info",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> paymentInfoSink = new FlinkKafkaProducer<>(
                "ods_payment_info",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> consultOrderLinkSink = new FlinkKafkaProducer<>(
                "ods_consult_order_link",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> productInfoSink = new FlinkKafkaProducer<>(
                "ods_product_info",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> shopInfoSink = new FlinkKafkaProducer<>(
                "ods_shop_info",
                new SimpleStringSchema(),
                props
        );

        FlinkKafkaProducer<String> csInfoSink = new FlinkKafkaProducer<>(
                "ods_cs_info",
                new SimpleStringSchema(),
                props
        );

        // 7. 将侧输出流连接到对应的Kafka主题
        processedStream.getSideOutput(CONSULT_LOG_TAG).addSink(consultLogSink).name("Consult Log Sink");
        processedStream.getSideOutput(ORDER_INFO_TAG).addSink(orderInfoSink).name("Order Info Sink");
        processedStream.getSideOutput(PAYMENT_INFO_TAG).addSink(paymentInfoSink).name("Payment Info Sink");
        processedStream.getSideOutput(CONSULT_ORDER_LINK_TAG).addSink(consultOrderLinkSink).name("Consult Order Link Sink");
        processedStream.getSideOutput(PRODUCT_INFO_TAG).addSink(productInfoSink).name("Product Info Sink");
        processedStream.getSideOutput(SHOP_INFO_TAG).addSink(shopInfoSink).name("Shop Info Sink");
        processedStream.getSideOutput(CS_INFO_TAG).addSink(csInfoSink).name("CS Info Sink");

        // 8. 启动作业
        env.execute("ODS MySQL CDC → Kafka");
    }
}