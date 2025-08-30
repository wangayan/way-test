package com.gd.gd02.dws;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

/**
 * @Title: ProductPaymentAggregateFunction
 * @Author wang.Ayan
 * @Date 2025/8/30 11:44
 * @Package com.gd.gd02.dws
 * @description: 商品支付聚合函数
 */
public class ProductPaymentAggregateFunction implements AggregateFunction<JSONObject, JSONObject, JSONObject> {

    @Override
    public JSONObject createAccumulator() {
        JSONObject acc = new JSONObject();
        acc.put("payment_count", 0L);
        acc.put("payment_user_count", 0L);
        acc.put("payment_amount", BigDecimal.ZERO);
        acc.put("payment_users", new HashSet<String>());
        return acc;
    }

    @Override
    public JSONObject add(JSONObject value, JSONObject accumulator) {
        String buyerId = value.getString("buyer_id");
        BigDecimal amount = value.getBigDecimal("amount");

        // 实际应用中需要通过订单ID找到对应的商品ID
        // 这里简化处理
        accumulator.put("product_id", "unknown_product");
        accumulator.put("date", value.getString("payment_time").substring(0, 10)); // 提取日期部分

        long paymentCount = accumulator.getLongValue("payment_count") + 1;
        accumulator.put("payment_count", paymentCount);

        BigDecimal paymentAmount = accumulator.getBigDecimal("payment_amount").add(amount);
        accumulator.put("payment_amount", paymentAmount);

        Set<String> paymentUsers = (Set<String>) accumulator.get("payment_users");
        if (paymentUsers.add(buyerId)) {
            accumulator.put("payment_user_count", paymentUsers.size());
        }

        return accumulator;
    }

    @Override
    public JSONObject getResult(JSONObject accumulator) {
        // 移除不需要的中间数据
        accumulator.remove("payment_users");
        return accumulator;
    }

    @Override
    public JSONObject merge(JSONObject a, JSONObject b) {
        // 合并两个累加器
        long paymentCount = a.getLongValue("payment_count") + b.getLongValue("payment_count");
        a.put("payment_count", paymentCount);

        BigDecimal paymentAmount = a.getBigDecimal("payment_amount").add(b.getBigDecimal("payment_amount"));
        a.put("payment_amount", paymentAmount);

        Set<String> paymentUsersA = (Set<String>) a.get("payment_users");
        Set<String> paymentUsersB = (Set<String>) b.get("payment_users");
        paymentUsersA.addAll(paymentUsersB);
        a.put("payment_user_count", paymentUsersA.size());

        return a;
    }
}
