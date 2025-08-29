package com.gd.gd02.dws;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Title: ConsultOrderLinkFunction
 * @description: 咨询订单关联函数
 * 工单编号：大数据-用户画像-02-服务主题店铺绩效
 */
public class ConsultOrderLinkFunction extends CoProcessFunction<JSONObject, JSONObject, JSONObject> {
    private transient ValueState<JSONObject> consultState;
    private transient ValueState<JSONObject> orderState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<JSONObject> consultDescriptor =
                new ValueStateDescriptor<>("consultState", JSONObject.class);
        consultState = getRuntimeContext().getState(consultDescriptor);

        ValueStateDescriptor<JSONObject> orderDescriptor =
                new ValueStateDescriptor<>("orderState", JSONObject.class);
        orderState = getRuntimeContext().getState(orderDescriptor);
    }

    @Override
    public void processElement1(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 处理第一个流的数据（咨询订单关联表）
        String eventType = value.getString("event_type");

        if ("consult".equals(eventType)) {
            consultState.update(value);
        }

        // 检查是否可以输出结果
        checkAndOutputResult(out);
    }

    @Override
    public void processElement2(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 处理第二个流的数据（咨询日志）
        String eventType = value.getString("event_type");

        if ("order".equals(eventType)) {
            orderState.update(value);
        }

        // 检查是否可以输出结果
        checkAndOutputResult(out);
    }

    private void checkAndOutputResult(Collector<JSONObject> out) throws Exception {
        // 当咨询和订单信息都齐全时，计算关联指标
        JSONObject consult = consultState.value();
        JSONObject order = orderState.value();

        if (consult != null && order != null) {
            JSONObject result = new JSONObject();
            result.put("shop_id", consult.getString("shop_id"));
            result.put("consult_id", consult.getString("consult_id"));
            result.put("order_id", order.getString("order_id"));
            result.put("buyer_id", consult.getString("buyer_id"));

            // 计算咨询到订单的转化
            result.put("consult_to_order", 1);
            result.put("type", "consult_order_link");
            result.put("process_time", System.currentTimeMillis());

            out.collect(result);

            // 清空状态
            consultState.clear();
            orderState.clear();
        }
    }
}