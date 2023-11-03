package com.busleiman.qbank.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;

@Data
@Builder
public class BankAccount {

    @Id
    private String userDNI;

    private Long usd;

    private Long ordersExecuted;

}
