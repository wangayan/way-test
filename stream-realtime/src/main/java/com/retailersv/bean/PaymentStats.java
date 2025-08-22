package com.retailersv.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class PaymentStats {
    private String stt;
    private String edt;
    private String sku_id;
    private Long payment_ct;
    private Long payment_user_ct;
    private Double payment_amount;
}
