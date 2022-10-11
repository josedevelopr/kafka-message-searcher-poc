package com.rdlnbank.customerapp.web;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rdlnbank.customerapp.kafka.consumer.SearchByTime;
import com.rdlnbank.customerapp.kafka.consumer.TimeRangeConsumer;
import com.rdlnbank.customerapp.kafka.producer.KafkaProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequiredArgsConstructor
@Slf4j
public class KafkaController {
  private final KafkaProducer kafkaProducer;
  public final ObjectMapper objectMapper = new ObjectMapper();
  @PostMapping("/messages/send")
  public ResponseEntity<String> sendMessage(@RequestBody String transaction) {
    kafkaProducer.sendMessage(transaction);
    return ResponseEntity.ok(transaction);
  }

  @GetMapping(
    value = "/messages/search",
    produces = MediaType.APPLICATION_JSON_VALUE
  )
  public List searchMessages(
    @RequestHeader("from") String from,
    @RequestHeader("until") String until) {
    try {
      return TimeRangeConsumer.consumeMessages(from, until)
        .stream()
        .map(stringJson -> {
          try {
            return objectMapper.readTree(stringJson);
          } catch (JsonProcessingException exception) {
            log.error("Error : " + exception);
          }
          return null;
        })
        .collect(Collectors.toList());
    } catch (Exception e) {
      log.error("Error : {} ", e);
    }
    return Collections.EMPTY_LIST;
  }
}
