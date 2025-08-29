package com.gd.gd02.dws;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @Title: ConsultAggregateFunction
 * @Author wang.Ayan
 * @Date 2025/8/29 18:53
 * @Package com.gd.gd02.dws
 * @description: 咨询数据聚合函数
 */
public class ConsultAggregateFunction implements AggregateFunction<JSONObject, Map<String, Object>, JSONObject> {
        @Override
        public Map<String, Object> createAccumulator() {
            Map<String, Object> acc = new HashMap<>();
            acc.put("consult_count", 0);
            acc.put("consult_user_set", new HashSet<String>());
            acc.put("shop_id", "");
            acc.put("date", "");
            return acc;
        }

        @Override
        public Map<String, Object> add(JSONObject value, Map<String, Object> accumulator) {
            accumulator.put("consult_count", (Integer) accumulator.get("consult_count") + 1);

            Set<String> userSet = (Set<String>) accumulator.get("consult_user_set");
            userSet.add(value.getString("buyer_id"));
            accumulator.put("consult_user_set", userSet);

            if (accumulator.get("shop_id").equals("")) {
                accumulator.put("shop_id", value.getString("shop_id"));

                String consultTime = value.getString("consult_time");
                String date = consultTime.split(" ")[0];
                accumulator.put("date", date);
            }

            return accumulator;
        }

        @Override
        public JSONObject getResult(Map<String, Object> accumulator) {
            JSONObject result = new JSONObject();
            result.put("shop_id", accumulator.get("shop_id"));
            result.put("date", accumulator.get("date"));
            result.put("consult_count", accumulator.get("consult_count"));

            Set<String> userSet = (Set<String>) accumulator.get("consult_user_set");
            result.put("consult_user_count", userSet.size());
            result.put("type", "consult_stats");
            result.put("process_time", System.currentTimeMillis());

            return result;
        }

        @Override
        public Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b) {
            a.put("consult_count", (Integer) a.get("consult_count") + (Integer) b.get("consult_count"));

            Set<String> userSetA = (Set<String>) a.get("consult_user_set");
            Set<String> userSetB = (Set<String>) b.get("consult_user_set");
            userSetA.addAll(userSetB);
            a.put("consult_user_set", userSetA);

            return a;
        }
    }