package com.busleiman.qbank.dto;

import com.busleiman.qbank.model.OrderState;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@SuperBuilder
public class OrderConfirmation {
    private Long id;
    private String sellerDni;
    private String orderState;
    private String errorDescription;
}
