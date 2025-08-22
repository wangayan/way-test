package com.stream.utils;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
public class ProcessCdcBusinessFunc extends RichMapFunction<JSONObject, String> {

    private final String table;

    public ProcessCdcBusinessFunc(String table) {
        this.table = table;
    }

    @Override
    public String map(JSONObject json) throws Exception {
        String op = json.getString("op");
        JSONObject after = json.getJSONObject("after");
        if (after == null) return null;

        JSONObject res = new JSONObject();
        res.put("table", table);
        res.put("op", op);
        res.put("ts", System.currentTimeMillis());

        if ("order_info".equals(table)) {
            res.put("order_id", after.getString("id"));
            res.put("user_id", after.getString("user_id"));
            res.put("sku_id", after.getString("sku_id"));
            res.put("province_id", after.getString("province_id"));
            res.put("total_amount", after.getString("total_amount"));
            res.put("create_time", after.getString("create_time"));
        } else if ("payment_info".equals(table)) {
            res.put("payment_id", after.getString("id"));
            res.put("order_id", after.getString("order_id"));
            res.put("user_id", after.getString("user_id"));
            res.put("total_amount", after.getString("total_amount"));
            res.put("payment_type", after.getString("payment_type"));
            res.put("payment_time", after.getString("payment_time"));
        } else if ("refund_info".equals(table)) {
            res.put("refund_id", after.getString("id"));
            res.put("order_id", after.getString("order_id"));
            res.put("user_id", after.getString("user_id"));
            res.put("refund_amount", after.getString("refund_amount"));
            res.put("refund_type", after.getString("refund_type"));
            res.put("refund_time", after.getString("refund_time"));
        }

        return res.toJSONString();
    }
}
