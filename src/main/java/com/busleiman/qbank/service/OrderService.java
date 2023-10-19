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
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
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
import java.util.Date;


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

        return receiver.consumeAutoAck(QUEUE).flatMap(message -> {

            String json = new String(message.getBody(), StandardCharsets.UTF_8);
            OrderRequest orderRequest;

            try {
                orderRequest = objectMapper.readValue(json, OrderRequest.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

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

                                                Flux<OutboundMessage> outbound = outboundMessage(walletRequest);

                                                return sender
                                                        .declareQueue(QueueSpecification.queue(QUEUE2))
                                                        .thenMany(sender.sendWithPublishConfirms(outbound))
                                                        .subscribe();
                                            });
                                });
                    }).switchIfEmpty(Mono.error(new Exception("User not found")));
        }).subscribe();
    }


    private Flux<OutboundMessage> outboundMessage(WalletRequest walletRequest) {

        String json1;
        try {
            json1 = objectMapper.writeValueAsString(walletRequest);

            long now = System.currentTimeMillis();
            long expirationTime = now + 3600000;
            String subject = walletRequest.getBuyerDni();

            String jwt = Jwts.builder()
                    .setSubject(subject)
                    .setIssuedAt(new Date(now))
                    .setExpiration(new Date(expirationTime))
                    .signWith(SignatureAlgorithm.HS256, "mySecretKey1239876123456123123123123132131231")
                    .claim("message", json1)
                    .compact();

            return Flux.just(new OutboundMessage(
                    "",
                    QUEUE2,
                    jwt.getBytes()));

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}

