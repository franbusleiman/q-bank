package com.busleiman.qbank.dto;


import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class WalletRequest extends OrderRequest {
    private String orderId;
}
