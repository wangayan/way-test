package com.gd.gd02.dws;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

/**
 * @Title: ProductOrderAggregateFunction
 * @Author wang.Ayan
 * @Date 2025/8/30 11:40
 * @Package com.gd.gd02.dws
 * @description: 商品订单聚合函数
 */
public class ProductOrderAggregateFunction implements AggregateFunction<JSONObject, JSONObject, JSONObject> {

    @Override
    public JSONObject createAccumulator() {
        JSONObject acc = new JSONObject();
        acc.put("order_count", 0L);
        acc.put("order_user_count", 0L);
        acc.put("order_amount", BigDecimal.ZERO);
        acc.put("order_users", new HashSet<String>());
        return acc;
    }

    @Override
    public JSONObject add(JSONObject value, JSONObject accumulator) {
        String productId = value.getString("product_id");
        String buyerId = value.getString("buyer_id");
        BigDecimal amount = value.getBigDecimal("amount");

        accumulator.put("product_id", productId);
        accumulator.put("date", value.getString("order_time").substring(0, 10)); // 提取日期部分

        long orderCount = accumulator.getLongValue("order_count") + 1;
        accumulator.put("order_count", orderCount);

        BigDecimal orderAmount = accumulator.getBigDecimal("order_amount").add(amount);
        accumulator.put("order_amount", orderAmount);

        Set<String> orderUsers = (Set<String>) accumulator.get("order_users");
        if (orderUsers.add(buyerId)) {
            accumulator.put("order_user_count", orderUsers.size());
        }

        return accumulator;
    }

    @Override
    public JSONObject getResult(JSONObject accumulator) {
        // 移除不需要的中间数据
        accumulator.remove("order_users");
        return accumulator;
    }

    @Override
    public JSONObject merge(JSONObject a, JSONObject b) {
        // 合并两个累加器
        long orderCount = a.getLongValue("order_count") + b.getLongValue("order_count");
        a.put("order_count", orderCount);

        BigDecimal orderAmount = a.getBigDecimal("order_amount").add(b.getBigDecimal("order_amount"));
        a.put("order_amount", orderAmount);

        Set<String> orderUsersA = (Set<String>) a.get("order_users");
        Set<String> orderUsersB = (Set<String>) b.get("order_users");
        orderUsersA.addAll(orderUsersB);
        a.put("order_user_count", orderUsersA.size());

        return a;
    }
}
