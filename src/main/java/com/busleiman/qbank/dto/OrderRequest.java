package com.busleiman.qbank.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class OrderRequest {
    private Long id;
    private String buyerDni;
    private String sellerDni;
    private Long usdAmount;
    private Long javaCoinPrice;
}
