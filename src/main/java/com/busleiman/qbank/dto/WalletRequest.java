package com.busleiman.qbank.dto;


import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@SuperBuilder
public class WalletRequest extends OrderRequest {
    private Long id;
}
