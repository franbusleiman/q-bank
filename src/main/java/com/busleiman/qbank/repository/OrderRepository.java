package com.busleiman.qbank.repository;

import com.busleiman.qbank.model.Order;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderRepository extends R2dbcRepository<Order, String> {
}
