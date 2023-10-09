package com.busleiman.qbank.model;

import lombok.Data;

@Data
public class Order {
    private String id;
    private OrderState orderState;
}
