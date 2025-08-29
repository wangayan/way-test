package com.gd.gd02.dws;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;


/**
 * @Title: ShopPerformanceAggregateFunction
 * @Author wang.Ayan
 * @Date 2025/8/29 18:59
 * @Package com.gd.gd02.dws
 * @description: 店铺绩效最终聚合函数
 */
public class ShopPerformanceAggregateFunction implements AggregateFunction<JSONObject, Map<String, Object>, JSONObject> {
    @Override
    public Map<String, Object> createAccumulator() {
        Map<String, Object> acc = new HashMap<>();
        acc.put("shop_id", "");
        acc.put("date", "");
        acc.put("consult_count", 0);
        acc.put("consult_user_count", 0);
        acc.put("order_count", 0);
        acc.put("order_user_count", 0);
        acc.put("order_amount", 0.0);
        acc.put("payment_count", 0);
        acc.put("payment_user_count", 0);
        acc.put("payment_amount", 0.0);
        acc.put("consult_to_order_count", 0);
        return acc;
    }

    @Override
    public Map<String, Object> add(JSONObject value, Map<String, Object> accumulator) {
        String type = value.getString("type");

        if (accumulator.get("shop_id").equals("")) {
            accumulator.put("shop_id", value.getString("shop_id"));
            accumulator.put("date", value.getString("date"));
        }

        switch (type) {
            case "consult_stats":
                accumulator.put("consult_count", value.getInteger("consult_count"));
                accumulator.put("consult_user_count", value.getInteger("consult_user_count"));
                break;
            case "order_stats":
                accumulator.put("order_count", value.getInteger("order_count"));
                accumulator.put("order_user_count", value.getInteger("order_user_count"));
                accumulator.put("order_amount", value.getDouble("order_amount"));
                break;
            case "payment_stats":
                accumulator.put("payment_count", value.getInteger("payment_count"));
                accumulator.put("payment_user_count", value.getInteger("payment_user_count"));
                accumulator.put("payment_amount", value.getDouble("payment_amount"));
                break;
            case "consult_order_link":
                accumulator.put("consult_to_order_count", (Integer) accumulator.get("consult_to_order_count") + 1);
                break;
        }

        return accumulator;
    }

    @Override
    public JSONObject getResult(Map<String, Object> accumulator) {
        JSONObject result = new JSONObject();
        result.put("shop_id", accumulator.get("shop_id"));
        result.put("date", accumulator.get("date"));
        result.put("consult_count", accumulator.get("consult_count"));
        result.put("consult_user_count", accumulator.get("consult_user_count"));
        result.put("order_count", accumulator.get("order_count"));
        result.put("order_user_count", accumulator.get("order_user_count"));
        result.put("order_amount", accumulator.get("order_amount"));
        result.put("payment_count", accumulator.get("payment_count"));
        result.put("payment_user_count", accumulator.get("payment_user_count"));
        result.put("payment_amount", accumulator.get("payment_amount"));
        result.put("consult_to_order_count", accumulator.get("consult_to_order_count"));

        // 计算转化率
        int consultUserCount = (Integer) accumulator.get("consult_user_count");
        int consultToOrderCount = (Integer) accumulator.get("consult_to_order_count");
        int orderCount = (Integer) accumulator.get("order_count");
        int paymentCount = (Integer) accumulator.get("payment_count");

        // 询单->下单转化率
        if (consultUserCount > 0) {
            result.put("consult_to_order_rate", (double) consultToOrderCount / consultUserCount);
        } else {
            result.put("consult_to_order_rate", 0.0);
        }

        // 下单->付款转化率
        if (orderCount > 0) {
            result.put("order_to_payment_rate", (double) paymentCount / orderCount);
        } else {
            result.put("order_to_payment_rate", 0.0);
        }

        // 询单->付款转化率
        if (consultUserCount > 0) {
            result.put("consult_to_payment_rate", (double) paymentCount / consultUserCount);
        } else {
            result.put("consult_to_payment_rate", 0.0);
        }

        result.put("process_time", System.currentTimeMillis());
        return result;
    }

    @Override
    public Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b) {
        a.put("consult_count", (Integer) a.get("consult_count") + (Integer) b.get("consult_count"));
        a.put("consult_user_count", (Integer) a.get("consult_user_count") + (Integer) b.get("consult_user_count"));
        a.put("order_count", (Integer) a.get("order_count") + (Integer) b.get("order_count"));
        a.put("order_user_count", (Integer) a.get("order_user_count") + (Integer) b.get("order_user_count"));
        a.put("order_amount", (Double) a.get("order_amount") + (Double) b.get("order_amount"));
        a.put("payment_count", (Integer) a.get("payment_count") + (Integer) b.get("payment_count"));
        a.put("payment_user_count", (Integer) a.get("payment_user_count") + (Integer) b.get("payment_user_count"));
        a.put("payment_amount", (Double) a.get("payment_amount") + (Double) b.get("payment_amount"));
        a.put("consult_to_order_count", (Integer) a.get("consult_to_order_count") + (Integer) b.get("consult_to_order_count"));

        return a;
    }
}
