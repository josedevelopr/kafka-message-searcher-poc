package com.rdlnbank.customerapp.kafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component @RequiredArgsConstructor @Slf4j public class KafkaProducer {
  private final KafkaTemplate<String, String> kafkaTemplate;

  @Value(value = "${kafka.topic.name}") private String topic;

  public void sendMessage(String message) {
    Message<String> messageToPublish = MessageBuilder
      .withPayload(message)
      .setHeader(KafkaHeaders.TOPIC, this.topic)
      .setHeader("test-case", "TC-0001")
      .build();
    ListenableFuture<org.springframework.kafka.support.SendResult<String, String>> future =
      kafkaTemplate.send(messageToPublish);
    future.addCallback(new ListenableFutureCallback<>() {
      @Override public void onFailure(Throwable ex) {
        log.info("Something went wrong with the message {} - Error: {}", message, ex.getMessage());
      }

      @Override public void onSuccess(SendResult<String, String> result) {
        log.info("Message {} has been sent to the topic {}", message, topic);
      }
    });
  }
}
