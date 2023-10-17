package com.busleiman.qbank.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class OrderRequest {
    private String buyerDni;
    private String sellerDni;
    private Long usdAmount;
    private Long javaCoinPrice;
}
