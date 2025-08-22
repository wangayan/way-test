package com.retailersv.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
    public class OrderStats {
        private String stt;
        private String edt;
        private String sku_id;
        private Long order_ct;
        private Long order_user_ct;
        private Long sku_num;
        private Double order_amount;
    }





