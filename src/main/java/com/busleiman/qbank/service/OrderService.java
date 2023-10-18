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
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.Sender;

import java.nio.charset.StandardCharsets;


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
    private static final String QUEUE = "Francisco";
    private static final String QUEUE2 = "Francisco2";

    private ObjectMapper objectMapper = new ObjectMapper();

    public OrderService(BankAccountRepository bankAccountRepository, OrderRepository orderRepository,
                        ModelMapper modelMapper, Receiver receiver, Sender sender) {
        this.bankAccountRepository = bankAccountRepository;
        this.orderRepository = orderRepository;
        this.modelMapper = modelMapper;
        this.receiver = receiver;
        this.sender = sender;
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
                                            .map(order1 -> {
                                                WalletRequest walletRequest = modelMapper.map(order1, WalletRequest.class);

                                                System.out.printf(String.valueOf(walletRequest));

                                                String json1;
                                                try {
                                                    json1 = objectMapper.writeValueAsString(walletRequest);

                                                    byte[] orderSerialized = SerializationUtils.serialize(json1);

                                                    Flux<OutboundMessage> outbound = Flux.just(new OutboundMessage(
                                                            "",
                                                            QUEUE2, orderSerialized));

                                                    // Declare the queue then send the flux of messages.
                                                    sender
                                                            .declareQueue(QueueSpecification.queue(QUEUE2))
                                                            .thenMany(sender.sendWithPublishConfirms(outbound))
                                                            .subscribe(m -> {
                                                                System.out.println("Message sent");
                                                            });

                                                } catch (JsonProcessingException e) {
                                                    throw new RuntimeException(e);
                                                }

                                                return walletRequest;
                                            });
                                });
                    }).switchIfEmpty(Mono.error(new Exception("User not found")));
        }).subscribe();
    }
}
