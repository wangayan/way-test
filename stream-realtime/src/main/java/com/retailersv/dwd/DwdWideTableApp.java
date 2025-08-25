package com.retailersv.dwd;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.Data;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
@Data
class OrderWideDetail implements Serializable {
    private String orderDetailId;
    private String orderId;
    private String userId;
    private String skuId;
    private String eventType;
    private Double orderAmount;
    private Double payAmount;
    private Double refundAmount;
    private Long ts;
}


public class DwdWideTableApp {
    private static final String KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.enableCheckpointing(10000L);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-dwd");

        DataStreamSource<String> orderStream = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_SERVER, "dwd_trade_order_detail", "dwd_fact_wide_group", OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(), "order_source");

        DataStreamSource<String> payStream = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_SERVER, "dwd_trade_pay_detail", "dwd_fact_wide_group", OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(), "pay_source");

        DataStreamSource<String> refundStream = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_SERVER, "dwd_trade_order_refund", "dwd_fact_wide_group", OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(), "refund_source");

        // 订单明细（包含 order_detail_id）
        SingleOutputStreamOperator<OrderWideDetail> orderDetailStream = orderStream.map(line -> {
            JSONObject obj = JSONObject.parseObject(line);
            JSONObject data = obj.getJSONObject("data");
            if (data == null) data = obj.getJSONObject("after");

            OrderWideDetail wide = new OrderWideDetail();
            if (data != null) {
                wide.setOrderDetailId(data.getString("id")); // 👈 id 就是 order_detail_id
                wide.setOrderId(data.getString("order_id"));
                wide.setUserId(data.getString("user_id"));
                wide.setSkuId(data.getString("sku_id"));
                wide.setOrderAmount(data.getDouble("total_amount"));
                wide.setEventType("order");
                Long ts = data.getLong("create_time");
                if (ts == null) ts = obj.getLong("ts");
                wide.setTs(ts != null ? ts : System.currentTimeMillis());
            }
            return wide;
        });

// 支付明细（来自 dwd_trade_pay_detail）
        SingleOutputStreamOperator<OrderWideDetail> payDetailStream = payStream.map(line -> {
            JSONObject obj = JSONObject.parseObject(line);
            JSONObject data = obj.getJSONObject("data");
            if (data == null) data = obj.getJSONObject("after");

            OrderWideDetail wide = new OrderWideDetail();
            if (data != null) {
                wide.setOrderDetailId(data.getString("order_detail_id")); // 👈 显式设置
                wide.setOrderId(data.getString("order_id"));
                wide.setUserId(data.getString("user_id"));
                wide.setPayAmount(data.getDouble("total_amount"));
                wide.setEventType("pay");
                Long ts = data.getLong("callback_time");
                if (ts == null) ts = data.getLong("create_time");
                if (ts == null) ts = obj.getLong("ts");
                wide.setTs(ts != null ? ts : System.currentTimeMillis());
            }
            return wide;
        });

// 退款明细（来自 dwd_trade_order_refund）
        SingleOutputStreamOperator<OrderWideDetail> refundDetailStream = refundStream.map(line -> {
            JSONObject obj = JSONObject.parseObject(line);
            JSONObject data = obj.getJSONObject("data");
            if (data == null) data = obj.getJSONObject("after");

            OrderWideDetail wide = new OrderWideDetail();
            if (data != null) {
                wide.setOrderDetailId(data.getString("order_detail_id")); // 👈 显式设置
                wide.setOrderId(data.getString("order_id"));
                wide.setUserId(data.getString("user_id"));
                wide.setSkuId(data.getString("sku_id"));
                wide.setRefundAmount(data.getDouble("total_amount"));
                wide.setEventType("refund");
                Long ts = data.getLong("callback_time");
                if (ts == null) ts = data.getLong("create_time");
                if (ts == null) ts = obj.getLong("ts");
                wide.setTs(ts != null ? ts : System.currentTimeMillis());
            }
            return wide;
        });



        // 合并三流
        DataStream<OrderWideDetail> unifiedStream = orderDetailStream
                .union(payDetailStream)
                .union(refundDetailStream);

// 过滤 null，再按 orderDetailId 处理
        DataStream<OrderWideDetail> result = unifiedStream
                .filter(o -> o.getOrderDetailId() != null) // 👈 关键点
                .keyBy(OrderWideDetail::getOrderDetailId)
                .process(new KeyedProcessFunction<String, OrderWideDetail, OrderWideDetail>() {
                    private ValueState<OrderWideDetail> state;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<OrderWideDetail> descriptor = new ValueStateDescriptor<>(
                                "order_detail_state", TypeInformation.of(OrderWideDetail.class));
                        state = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(OrderWideDetail input, Context ctx, Collector<OrderWideDetail> out) throws Exception {
                        OrderWideDetail current = state.value();
                        if (current == null) {
                            current = new OrderWideDetail();
                            current.setOrderDetailId(input.getOrderDetailId());
                        }
                        if (input.getOrderId() != null) current.setOrderId(input.getOrderId());
                        if (input.getUserId() != null) current.setUserId(input.getUserId());
                        if (input.getSkuId() != null) current.setSkuId(input.getSkuId());
                        if (input.getOrderAmount() != null) current.setOrderAmount(input.getOrderAmount());
                        if (input.getPayAmount() != null) current.setPayAmount(input.getPayAmount());
                        if (input.getRefundAmount() != null) current.setRefundAmount(input.getRefundAmount());
                        if (input.getTs() != null) current.setTs(input.getTs());

                        state.update(current);
                        out.collect(current);
                    }
                });

        result.print("dwd_wide_result ->");


        env.execute("DwdWideTableApp_FullUnion");
    }
}
