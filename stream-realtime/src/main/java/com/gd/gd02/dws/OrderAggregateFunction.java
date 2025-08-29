package com.gd.gd02.dws;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
/**
 * @Title: OrderAggregateFunction
 * @Author wang.Ayan
 * @Date 2025/8/29 18:55
 * @Package com.gd.gd02.dws
 * @description: 订单数据聚合函数
 */
public class OrderAggregateFunction implements AggregateFunction<JSONObject, Map<String, Object>, JSONObject> {
@Override
public Map<String, Object> createAccumulator() {
        Map<String, Object> acc = new HashMap<>();
        acc.put("order_count", 0);
        acc.put("order_user_set", new HashSet<String>());
        acc.put("order_amount", 0.0);
        acc.put("shop_id", "");
        acc.put("date", "");
        return acc;
        }

@Override
public Map<String, Object> add(JSONObject value, Map<String, Object> accumulator) {
        accumulator.put("order_count", (Integer) accumulator.get("order_count") + 1);

        Set<String> userSet = (Set<String>) accumulator.get("order_user_set");
        userSet.add(value.getString("buyer_id"));
        accumulator.put("order_user_set", userSet);

        Double amount = value.getDouble("amount");
        accumulator.put("order_amount", (Double) accumulator.get("order_amount") + amount);

        if (accumulator.get("shop_id").equals("")) {
        accumulator.put("shop_id", value.getString("shop_id"));

        String orderTime = value.getString("order_time");
        String date = orderTime.split(" ")[0];
        accumulator.put("date", date);
        }

        return accumulator;
        }

@Override
public JSONObject getResult(Map<String, Object> accumulator) {
        JSONObject result = new JSONObject();
        result.put("shop_id", accumulator.get("shop_id"));
        result.put("date", accumulator.get("date"));
        result.put("order_count", accumulator.get("order_count"));

        Set<String> userSet = (Set<String>) accumulator.get("order_user_set");
        result.put("order_user_count", userSet.size());
        result.put("order_amount", accumulator.get("order_amount"));
        result.put("type", "order_stats");
        result.put("process_time", System.currentTimeMillis());

        return result;
        }

@Override
public Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b) {
        a.put("order_count", (Integer) a.get("order_count") + (Integer) b.get("order_count"));
        a.put("order_amount", (Double) a.get("order_amount") + (Double) b.get("order_amount"));

        Set<String> userSetA = (Set<String>) a.get("order_user_set");
        Set<String> userSetB = (Set<String>) b.get("order_user_set");
        userSetA.addAll(userSetB);
        a.put("order_user_set", userSetA);

        return a;
    }
}