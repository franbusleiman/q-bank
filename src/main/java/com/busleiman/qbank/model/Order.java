package com.busleiman.qbank.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;

@Data
@Builder
public class Order {
    @Id
    private String id;
    private String buyerDni;
    private String sellerDni;
    private  Long usdAmount;
    private Long javaCoinPrice;
    private OrderState orderState;
}
