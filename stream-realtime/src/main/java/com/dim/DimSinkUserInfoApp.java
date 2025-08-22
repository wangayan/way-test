package com.dim;//package com.dim;
//
//import com.alibaba.fastjson.JSONObject;
//import com.stream.common.utils.HbaseUtils;
//import com.stream.common.utils.KafkaUtils;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.ProcessFunction;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.util.Collector;
//import org.apache.flink.util.OutputTag;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.BufferedMutator;
//import org.apache.hadoop.hbase.client.BufferedMutatorParams;
//import org.apache.hadoop.hbase.client.Connection;
//
//public class DimSinkUserInfoApp {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        KafkaSource<String> source = KafkaUtils.getKafkaSource(
//                "ods_mysql_tab",
//                "dim_user_info_group",
//                "cdh01:9092"
//        );
//
//        SingleOutputStreamOperator<JSONObject> jsonStream = env
//                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka_Source")
//                .map(JSONObject::parseObject)
//                .uid("Parse_JSON")
//                .name("Parse_JSON");
//
//       //jsonStream.print("jsonStream");
//
//        OutputTag<JSONObject> userInfoTag = new OutputTag<JSONObject>("user_info_tag") {};
//
//        SingleOutputStreamOperator<JSONObject> mainStream = jsonStream.process(new ProcessFunction<JSONObject, JSONObject>() {
//            @Override
//            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) {
//                JSONObject after = value.getJSONObject("after");
//                if (after != null && after.containsKey("id")) {
//                    // 简单判断可能是 user_info 表的数据（你也可以更精细）
//                    ctx.output(userInfoTag, value);
//                }
//            }
//        });
//
//        mainStream.getSideOutput(userInfoTag).print("userInfoStream");
//
//
//
//        SideOutputDataStream<JSONObject> userInfoStream = mainStream.getSideOutput(userInfoTag);
//
//
//        // sink 到 HBase
//        userInfoStream.addSink(new RichSinkFunction<JSONObject>() {
//
//
//            transient HbaseUtils hbaseUtils;
//            transient BufferedMutator mutator;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                hbaseUtils = new HbaseUtils("cdh01,cdh02,cdh03");
//                Connection conn = hbaseUtils.getConnection();
//                BufferedMutatorParams params = new BufferedMutatorParams(
//                        TableName.valueOf("ns_zxn:dim_user_info")
//                );
//                mutator = conn.getBufferedMutator(params);
//            }
//
//            @Override
//            public void invoke(JSONObject value, Context context) throws Exception {
//                JSONObject after = value.getJSONObject("after");
//                if (after != null) {
//                    String userId = after.getString("id");
//                    if (userId != null) {
//                        HbaseUtils.put(userId, after, mutator);
//                    }
//                }
//            }
//
//            @Override
//            public void close() throws Exception {
//                if (mutator != null) mutator.close();
//                if (hbaseUtils != null && hbaseUtils.getConnection() != null) {
//                    hbaseUtils.getConnection().close();
//                }
//            }
//        });
//
//        env.execute("DimSinkUserInfoApp");
//    }
//}
