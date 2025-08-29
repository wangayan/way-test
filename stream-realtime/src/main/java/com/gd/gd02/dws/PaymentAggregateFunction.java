package com.gd.gd02.dws;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
/**
 * @Title: PaymentAggregateFunction
 * @Author wang.Ayan
 * @Date 2025/8/29 18:58
 * @Package com.gd.gd02.dws
 * @description: 支付数据聚合函数
 */
public class PaymentAggregateFunction implements AggregateFunction<JSONObject, Map<String, Object>, JSONObject> {
    @Override
    public Map<String, Object> createAccumulator() {
        Map<String, Object> acc = new HashMap<>();
        acc.put("payment_count", 0);
        acc.put("payment_user_set", new HashSet<String>());
        acc.put("payment_amount", 0.0);
        acc.put("shop_id", "");
        acc.put("date", "");
        return acc;
    }

    @Override
    public Map<String, Object> add(JSONObject value, Map<String, Object> accumulator) {
        accumulator.put("payment_count", (Integer) accumulator.get("payment_count") + 1);

        Set<String> userSet = (Set<String>) accumulator.get("payment_user_set");
        userSet.add(value.getString("buyer_id"));
        accumulator.put("payment_user_set", userSet);

        Double amount = value.getDouble("amount");
        accumulator.put("payment_amount", (Double) accumulator.get("payment_amount") + amount);

        if (accumulator.get("shop_id").equals("")) {
            // 支付信息中没有shop_id，需要通过订单ID关联获取
            // 这里简化处理，实际应用中需要通过订单ID查询店铺信息
            accumulator.put("shop_id", "unknown");

            String paymentTime = value.getString("payment_time");
            String date = paymentTime.split(" ")[0];
            accumulator.put("date", date);
        }

        return accumulator;
    }

    @Override
    public JSONObject getResult(Map<String, Object> accumulator) {
        JSONObject result = new JSONObject();
        result.put("shop_id", accumulator.get("shop_id"));
        result.put("date", accumulator.get("date"));
        result.put("payment_count", accumulator.get("payment_count"));

        Set<String> userSet = (Set<String>) accumulator.get("payment_user_set");
        result.put("payment_user_count", userSet.size());
        result.put("payment_amount", accumulator.get("payment_amount"));
        result.put("type", "payment_stats");
        result.put("process_time", System.currentTimeMillis());

        return result;
    }

    @Override
    public Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b) {
        a.put("payment_count", (Integer) a.get("payment_count") + (Integer) b.get("payment_count"));
        a.put("payment_amount", (Double) a.get("payment_amount") + (Double) b.get("payment_amount"));

        Set<String> userSetA = (Set<String>) a.get("payment_user_set");
        Set<String> userSetB = (Set<String>) b.get("payment_user_set");
        userSetA.addAll(userSetB);
        a.put("payment_user_set", userSetA);

        return a;
    }
}
