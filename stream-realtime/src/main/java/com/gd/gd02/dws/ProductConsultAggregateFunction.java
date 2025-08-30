package com.gd.gd02.dws;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashSet;
import java.util.Set;

/**
 * @Title: ProductConsultAggregateFunction
 * @Author wang.Ayan
 * @Date 2025/8/30 11:36
 * @Package com.gd.gd02.dws
 * @description: 商品咨询聚合函数
 */
public class ProductConsultAggregateFunction implements AggregateFunction<JSONObject, JSONObject, JSONObject> {

    @Override
    public JSONObject createAccumulator() {
        JSONObject acc = new JSONObject();
        acc.put("consult_count", 0L);
        acc.put("consult_user_count", 0L);
        acc.put("consult_users", new HashSet<String>());
        return acc;
    }

    @Override
    public JSONObject add(JSONObject value, JSONObject accumulator) {
        String productId = value.getString("product_id");
        String buyerId = value.getString("buyer_id");

        accumulator.put("product_id", productId);
        accumulator.put("date", value.getString("consult_time").substring(0, 10)); // 提取日期部分

        long consultCount = accumulator.getLongValue("consult_count") + 1;
        accumulator.put("consult_count", consultCount);

        Set<String> consultUsers = (Set<String>) accumulator.get("consult_users");
        if (consultUsers.add(buyerId)) {
            accumulator.put("consult_user_count", consultUsers.size());
        }

        return accumulator;
    }

    @Override
    public JSONObject getResult(JSONObject accumulator) {
        // 移除不需要的中间数据
        accumulator.remove("consult_users");
        return accumulator;
    }

    @Override
    public JSONObject merge(JSONObject a, JSONObject b) {
        // 合并两个累加器
        long consultCount = a.getLongValue("consult_count") + b.getLongValue("consult_count");
        a.put("consult_count", consultCount);

        Set<String> consultUsersA = (Set<String>) a.get("consult_users");
        Set<String> consultUsersB = (Set<String>) b.get("consult_users");
        consultUsersA.addAll(consultUsersB);
        a.put("consult_user_count", consultUsersA.size());

        return a;
    }
}
