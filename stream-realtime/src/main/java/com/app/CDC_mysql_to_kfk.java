package com.app;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CDC_mysql_to_kfk {
     public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        MySqlSource<String> mySQLCdcSource = CdcSourceUtils.getMySQLCdcSource("test", ".*", "root", "123456", StartupOptions.initial(), "10000-10050");
        env.fromSource(mySQLCdcSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        SingleOutputStreamOperator<JSONObject> parsedStream = env
                .fromSource(mySQLCdcSource, WatermarkStrategy.noWatermarks(), "MySQL_Source")
                .map(JSONObject::parseObject)
                .name("Parse_JSON");
        //parsedStream.print("parsedStream");

        SingleOutputStreamOperator<JSONObject> cleanedStream = parsedStream.map(json -> {
            json.remove("source");
            json.remove("transaction");
            return json;
        }).name("Clean_Columns");
        cleanedStream.print("cle");

        SingleOutputStreamOperator<JSONObject> filteredStream = cleanedStream

                .name("Filter_CUD");
        filteredStream.print("fil");
        filteredStream
                .map(obj -> JSONObject.toJSONString(obj))
                .returns(String.class)
                .sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "ods_mysql_tab"))
                .name("Sink_to_Kafka_ODS");

        env.execute();
    }
}
