package com.busleiman.qbank.service;

import com.busleiman.qbank.dto.OrderConfirmation;
import com.busleiman.qbank.dto.OrderRequest;
import com.busleiman.qbank.model.Order;
import com.busleiman.qbank.model.OrderState;
import com.busleiman.qbank.repository.BankAccountRepository;
import com.busleiman.qbank.repository.OrderRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Connection;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.Sender;

import java.nio.charset.StandardCharsets;

import static com.busleiman.qbank.utils.Constants.*;


@Service
@Slf4j
public class OrderService {

    @Autowired
    private BankAccountRepository bankAccountRepository;
    @Autowired
    private OrderRepository orderRepository;
    @Autowired
    private Mono<Connection> connectionMono;
    private final Receiver receiver;
    private final Sender sender;
    @Autowired
    private final ModelMapper modelMapper;

    private ObjectMapper objectMapper = new ObjectMapper();

    public OrderService(BankAccountRepository bankAccountRepository, OrderRepository orderRepository,
                        ModelMapper modelMapper, Receiver receiver, Sender sender) {
        this.bankAccountRepository = bankAccountRepository;
        this.orderRepository = orderRepository;
        this.modelMapper = modelMapper;
        this.receiver = receiver;
        this.sender = sender;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        consume();
        consume2();
    }

    @PreDestroy
    public void close() throws Exception {
        connectionMono.block().close();
    }


    public Disposable consume() {

        return receiver.consumeAutoAck(QUEUE_B).flatMap(message -> {

            String json = new String(message.getBody(), StandardCharsets.UTF_8);
            OrderRequest orderRequest;

            try {
                orderRequest = objectMapper.readValue(json, OrderRequest.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            return bankAccountRepository.findById(orderRequest.getBuyerDni())
                    .flatMap(bankAccount -> {
                        Long usdTotal;
                        Long buyerCommission;

                        if (bankAccount.getOrdersExecuted() < 3) {
                            usdTotal = (long) (orderRequest.getUsdAmount() * 1.05);
                            buyerCommission = (long) 1.05;
                        } else if (bankAccount.getOrdersExecuted() < 6) {
                            usdTotal = (long) (orderRequest.getUsdAmount() * 1.03);
                            buyerCommission = (long) 1.03;
                        } else {
                            usdTotal = (orderRequest.getUsdAmount());
                            buyerCommission = (long) 0;

                        }

                        if (usdTotal <= bankAccount.getUsd()) {
                            bankAccount.setUsd(bankAccount.getUsd() - usdTotal);

                            return bankAccountRepository.save(bankAccount)
                                    .flatMap(bankAccount1 -> {
                                        Order order = Order.builder()
                                                .id(orderRequest.getId())
                                                .javaCoinPrice(orderRequest.getJavaCoinPrice())
                                                .orderState(OrderState.IN_PROGRESS)
                                                .buyerDni(orderRequest.getBuyerDni())
                                                .usdAmount(orderRequest.getUsdAmount())
                                                .buyerCommission(buyerCommission)
                                                .build();
                                        return orderRepository.save(order);
                                    });
                        } else {
                            Order order = Order.builder()
                                    .id(orderRequest.getId())
                                    .javaCoinPrice(orderRequest.getJavaCoinPrice())
                                    .orderState(OrderState.NOT_ACCEPTED)
                                    .buyerDni(orderRequest.getBuyerDni())
                                    .usdAmount(orderRequest.getUsdAmount())
                                    .build();
                            return orderRepository.save(order);
                        }
                    }).switchIfEmpty(Mono.error(new Exception("User not found")));
        }).subscribe();
    }

    public Disposable consume2() {

        return receiver.consumeAutoAck(QUEUE_F).flatMap(message -> {

            String json = new String(message.getBody(), StandardCharsets.UTF_8);
            OrderConfirmation orderConfirmation;
            try {
                orderConfirmation = objectMapper.readValue(json, OrderConfirmation.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            return orderRepository.findById(orderConfirmation.getId())
                    .flatMap(order -> {

                        if (orderConfirmation.getOrderState().equals("NOT_ACCEPTED")) {

                            order.setOrderState(OrderState.NOT_ACCEPTED);

                            return bankAccountRepository.findById(order.getBuyerDni())
                                    .flatMap(buyerAccount -> {

                                        buyerAccount.setUsd(buyerAccount.getUsd() + order.getUsdAmount() * order.getBuyerCommission());

                                        return bankAccountRepository.save(buyerAccount)
                                                .then(orderRepository.save(order))

                                                .map(order1 -> {
                                                    OrderConfirmation orderConfirmation1 = modelMapper.map(order, OrderConfirmation.class);

                                                    Flux<OutboundMessage> outbound = outboundMessage(orderConfirmation1, QUEUE_E, QUEUES_EXCHANGE);

                                                    return sender.send(outbound)
                                                            .subscribe();
                                                });
                                    });

                        } else if (orderConfirmation.getOrderState().equals("ACCEPTED")) {

                            order.setOrderState(OrderState.ACCEPTED);
                            order.setSellerDni(orderConfirmation.getSellerDni());

                       return     bankAccountRepository.findById(order.getSellerDni())
                                    .flatMap(sellerAccount -> {

                                        return bankAccountRepository.findById(order.getBuyerDni())
                                                .flatMap(buyerAccount -> {

                                                    Long usdTotal;
                                                    if (sellerAccount.getOrdersExecuted() < 3) {
                                                        usdTotal = (long) (order.getUsdAmount() * 1.05);
                                                    } else if (sellerAccount.getOrdersExecuted() < 6) {
                                                        usdTotal = (long) (order.getUsdAmount() * 1.03);
                                                    } else {
                                                        usdTotal = (order.getUsdAmount());
                                                    }
                                                    sellerAccount.setUsd(sellerAccount.getUsd() + usdTotal);
                                                    sellerAccount.setOrdersExecuted(sellerAccount.getOrdersExecuted() + 1);
                                                    buyerAccount.setOrdersExecuted(buyerAccount.getOrdersExecuted() + 1);

                                                    return bankAccountRepository.save(sellerAccount)
                                                            .then(bankAccountRepository.save(buyerAccount))
                                                            .flatMap(result -> {
                                                                order.setOrderState(OrderState.ACCEPTED);

                                                                return orderRepository.save(order)
                                                                        .map(order1 -> {
                                                                            OrderConfirmation orderConfirmation1 = modelMapper.map(order, OrderConfirmation.class);

                                                                            Flux<OutboundMessage> outbound = outboundMessage(orderConfirmation1, QUEUE_E, QUEUES_EXCHANGE);

                                                                            return sender.send(outbound)
                                                                                    .subscribe();
                                                                        });
                                                            });
                                                }).switchIfEmpty(Mono.error(new Exception("User not found")));
                                    });
                        }
                        return Mono.error(new Exception("Order Status unknown: " + orderConfirmation.getOrderState()));

                    }).switchIfEmpty(Mono.error(new Exception("User not found")));
        }).subscribe();
    }


    private Flux<OutboundMessage> outboundMessage(Object message, String routingKey, String exchange) {

        String json;
        try {
            json = objectMapper.writeValueAsString(message);

            return Flux.just(new OutboundMessage(
                    exchange,
                    routingKey,
                    json.getBytes()));

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}

