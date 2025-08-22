package com.retailersv.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class RefundStats {
    private String stt;
    private String edt;
    private String sku_id;
    private Long refund_ct;
    private Long refund_user_ct;
    private Double refund_amount;
}
