package com.busleiman.qbank.service;

import com.busleiman.qbank.dto.OrderRequest;
import com.busleiman.qbank.dto.WalletRequest;
import com.busleiman.qbank.model.Order;
import com.busleiman.qbank.model.OrderState;
import com.busleiman.qbank.repository.BankAccountRepository;
import com.busleiman.qbank.repository.OrderRepository;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

@Service
public class OrderService {

    @Autowired
    private BankAccountRepository bankAccountRepository;
    @Autowired
    private OrderRepository orderRepository;

    private final ModelMapper modelMapper;

    public OrderService(BankAccountRepository bankAccountRepository, OrderRepository orderRepository, ModelMapper modelMapper) {
        this.bankAccountRepository = bankAccountRepository;
        this.orderRepository = orderRepository;
        this.modelMapper = modelMapper;
    }


    @Transactional
    public Mono<WalletRequest> processOrder(OrderRequest orderRequest) {

        return bankAccountRepository.findById(orderRequest.getBuyerDni())
                .flatMap(bankAccount -> {

                    Long usdTotal = (long) (orderRequest.getUsdAmount() * 1.05);

                    if (usdTotal <= bankAccount.getUsd()) {
                        bankAccount.setUsd(bankAccount.getUsd() - usdTotal);
                    }
                    return bankAccountRepository.save(bankAccount)
                            .flatMap(bankAccount1 -> {
                                Order order = Order.builder()
                                        .javaCoinPrice(orderRequest.getJavaCoinPrice())
                                        .orderState(OrderState.IN_PROGRESS)
                                        .sellerDni(orderRequest.getSellerDni())
                                        .buyerDni(orderRequest.getBuyerDni())
                                        .usdAmount(usdTotal)
                                        .build();
                               return orderRepository.save(order)
                                        .map(order1 ->  modelMapper.map(order1, WalletRequest.class));
                            });
                }).switchIfEmpty(Mono.error(new Exception("User not found")));
    }
}
