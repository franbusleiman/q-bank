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
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.Sender;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

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
        consume().subscribe();
        consume2().subscribe();
    }

    @PreDestroy
    public void close() throws Exception {
        connectionMono.block().close();
    }


    /**
     * Se recibe el mensaje order request por parte del servicio Web.
     * Se chequea si el usuario comprador existe, se calcula el total de dólares de la operación,
     * y la comisión que deberá pagar el comprador.
     * <p>
     * Si el saldo del comprador es suficiente, se guarda el mismo y se registra la orden,
     * si el saldo no es suficiente, se registra la orden fallida y se envía el mensaje de fondos
     * insuficientes.
     * Finalmente se envía el mensaje de confirmación al servicio Web.
     */
    public Flux<Void> consume() {

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
                        Double usdTotal;
                        Double buyerCommission;

                        if (bankAccount.getOrdersExecuted() < 3) {
                            usdTotal = orderRequest.getUsdAmount() * 1.05;
                            buyerCommission = 1.05;
                        } else if (bankAccount.getOrdersExecuted() < 6) {
                            usdTotal = orderRequest.getUsdAmount() * 1.03;
                            buyerCommission = 1.03;
                        } else {
                            usdTotal = (orderRequest.getUsdAmount());
                            buyerCommission = 0.0;
                        }

                        if (usdTotal <= bankAccount.getUsd()) {
                            bankAccount.setUsd(bankAccount.getUsd() - usdTotal);

                            return bankAccountRepository.save(bankAccount)
                                    .flatMap(bankAccount1 -> {
                                        Order order = Order.builder()
                                                .id(orderRequest.getId())
                                                .javaCoinPrice(orderRequest.getJavaCoinPrice())
                                                .orderState(OrderState.ACCEPTED)
                                                .buyerDni(orderRequest.getBuyerDni())
                                                .usdAmount(orderRequest.getUsdAmount())
                                                .buyerCommission(buyerCommission)
                                                .build();

                                        return orderRepository.save(order)
                                                .map(order1 -> modelMapper.map(order, OrderConfirmation.class));
                                    });
                        } else {
                            Order order = Order.builder()
                                    .id(orderRequest.getId())
                                    .javaCoinPrice(orderRequest.getJavaCoinPrice())
                                    .orderState(OrderState.NOT_ACCEPTED)
                                    .buyerDni(orderRequest.getBuyerDni())
                                    .usdAmount(orderRequest.getUsdAmount())
                                    .build();
                            return orderRepository.save(order)
                                    .map(order1 -> {
                                        OrderConfirmation orderConfirmation = modelMapper.map(order, OrderConfirmation.class);
                                        orderConfirmation.setErrorDescription("insufficient funds");
                                        return orderConfirmation;
                                    });
                        }
                    })
                    .switchIfEmpty(orderConfirmationError(orderRequest.getId(), null, "User not found: " + orderRequest.getBuyerDni()))
                    .flatMap(orderConfirmation1 -> {
                        Flux<OutboundMessage> outbound = outboundMessage(orderConfirmation1, QUEUE_G, QUEUES_EXCHANGE);


                        return sender.send(outbound);
                    });
        });
    }

    /**
     * Se recibe el mensaje order confirmation por parte del servicio Wallet.
     *<p>
     * Se chequea el estado de aceptado o no aceptado.
     * <p>
     * En el caso de que NO se acepto, se actualiza el estado de la orden y en el caso de que el error
     * no sea saldo insuficiente, se devuelve el dinero al comprador.
     * <p>
     * En el caso de que SI se acepto, se actualiza el estado de la orden, se le paga al vendedor el saldo,
     * y se aumenta la cantidad de operaciones realizadas tanto a comprador como a vendedor.
     *<p>
     * Finalmente se envía la confirmación al servicio Web.
     */
    public Flux<Void> consume2() {

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

                        if (orderConfirmation.getOrderState() == OrderState.NOT_ACCEPTED) {

                            order.setOrderState(OrderState.NOT_ACCEPTED);
                            order.setSellerDni(orderConfirmation.getSellerDni());

                            return bankAccountRepository.findById(order.getBuyerDni())
                                    .flatMap(buyerAccount -> {

                                        if (!Objects.equals(orderConfirmation.getErrorDescription(), "insufficient funds")){
                                            buyerAccount.setUsd(buyerAccount.getUsd() + order.getUsdAmount() * order.getBuyerCommission());
                                        }
                                        return bankAccountRepository.save(buyerAccount)
                                                .then(orderRepository.save(order))
                                                .map(order1 -> {
                                                    OrderConfirmation orderConfirmation1 = modelMapper.map(order, OrderConfirmation.class);
                                                    orderConfirmation1.setErrorDescription(orderConfirmation.getErrorDescription());
                                                    return orderConfirmation1;
                                                });
                                    });

                        } else if (orderConfirmation.getOrderState() == OrderState.ACCEPTED) {

                            order.setOrderState(OrderState.ACCEPTED);
                            order.setSellerDni(orderConfirmation.getSellerDni());

                            return bankAccountRepository.findById(order.getSellerDni())
                                    .flatMap(sellerAccount -> {

                                        return bankAccountRepository.findById(order.getBuyerDni())
                                                .flatMap(buyerAccount -> {

                                                    Double usdTotal = order.getUsdAmount();
                                                    Double commission = 0.0;

                                                    if (sellerAccount.getOrdersExecuted() < 3) {

                                                        commission = usdTotal * 0.05;

                                                    } else if (sellerAccount.getOrdersExecuted() < 6) {

                                                        commission = usdTotal * 0.03;
                                                    }
                                                    sellerAccount.setUsd(sellerAccount.getUsd() + (usdTotal - commission));
                                                    sellerAccount.setOrdersExecuted(sellerAccount.getOrdersExecuted() + 1);
                                                    buyerAccount.setOrdersExecuted(buyerAccount.getOrdersExecuted() + 1);

                                                    return bankAccountRepository.save(sellerAccount)
                                                            .then(bankAccountRepository.save(buyerAccount))
                                                            .flatMap(result -> {
                                                                order.setOrderState(OrderState.ACCEPTED);

                                                                return orderRepository.save(order)
                                                                        .map(order1 -> modelMapper.map(order, OrderConfirmation.class));
                                                            });
                                                }).switchIfEmpty(orderConfirmationError(order.getId(),
                                                        orderConfirmation.getSellerDni(), "User not found: " + order.getBuyerDni()));
                                    });
                        }
                        return orderConfirmationError(order.getId(),
                                orderConfirmation.getSellerDni(), "Order status unknown: " + orderConfirmation.getOrderState());

                    }).switchIfEmpty(orderConfirmationError(orderConfirmation.getId(),
                            orderConfirmation.getSellerDni(), "Order not found: " + orderConfirmation.getId()))

                    .flatMap(orderConfirmation1 -> {
                        Flux<OutboundMessage> outbound = outboundMessage(orderConfirmation1, QUEUE_E, QUEUES_EXCHANGE);
                        return sender.send(outbound);
                    });
        });
    }


    /**
     * Facilitador para crear mensajes de error.
     */
    public Mono<OrderConfirmation> orderConfirmationError(Long orderId, String sellerDni, String error) {
        return Mono.just(OrderConfirmation.builder()
                .id(orderId)
                .orderState(OrderState.NOT_ACCEPTED)
                .sellerDni(sellerDni)
                .errorDescription(error)
                .build());
    }

    /**
     * Se crea un mensaje, en el cual se especifica exchange, routing-key y cuerpo del mismo.
     */
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

