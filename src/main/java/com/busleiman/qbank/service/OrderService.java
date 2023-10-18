package com.busleiman.qbank.service;

import com.busleiman.qbank.dto.OrderRequest;
import com.busleiman.qbank.dto.WalletRequest;
import com.busleiman.qbank.model.Order;
import com.busleiman.qbank.model.OrderState;
import com.busleiman.qbank.repository.BankAccountRepository;
import com.busleiman.qbank.repository.OrderRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Connection;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.SerializationUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.Receiver;

import java.nio.charset.StandardCharsets;

@Service
public class OrderService {

    @Autowired
    private BankAccountRepository bankAccountRepository;
    @Autowired
    private OrderRepository orderRepository;
    @Autowired
    private Mono<Connection> connectionMono;
    private final Receiver receiver;
    @Autowired
    private final ModelMapper modelMapper;
    private static final String QUEUE = "Francisco";
    private ObjectMapper objectMapper = new ObjectMapper();

    public OrderService(BankAccountRepository bankAccountRepository, OrderRepository orderRepository,
                        ModelMapper modelMapper, Receiver receiver) {
        this.bankAccountRepository = bankAccountRepository;
        this.orderRepository = orderRepository;
        this.modelMapper = modelMapper;
        this.receiver = receiver;
    }

    @PostConstruct
    private void init() {
        consume();
    }

    @PreDestroy
    public void close() throws Exception {
        connectionMono.block().close();
    }


    public Disposable consume() {
        System.out.println("paso");

        return receiver.consumeAutoAck(QUEUE).flatMap(message -> {

            String json = new String(message.getBody(), StandardCharsets.UTF_8);
            System.out.println(json);

            OrderRequest orderRequest;


            try {
                orderRequest = objectMapper.readValue(json, OrderRequest.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }


            System.out.println(orderRequest);

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
                                            .map(order1 ->{
                                             WalletRequest walletRequest=    modelMapper.map(order1, WalletRequest.class)  ;

                                                System.out.printf(String.valueOf(walletRequest));

                                                return walletRequest;
                                            });
                                });
                    }).switchIfEmpty(Mono.error(new Exception("User not found")));
        }).subscribe();
    }
}
