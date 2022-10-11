package com.rdlnbank.customerapp.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.text.*;

public final class SearchByTime {
  public static void consumeMessagesFrom() {
    Properties kafkaProps = new Properties();
    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    kafkaProps.put("topic", "transaction-request");
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "transaction-group");
    kafkaProps.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaProps.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
    String topic = (String) kafkaProps.get("topic");
    kafkaConsumer.subscribe(Collections.singletonList(topic));
    System.out.println("subscribed to topic " + topic);
    System.out.println(kafkaConsumer.partitionsFor(topic));
    List<TopicPartition> partitions = new ArrayList<>();
    for (PartitionInfo p : kafkaConsumer.partitionsFor(topic)) {
      partitions.add(new TopicPartition(topic, p.partition()));
    }
    System.out.println(partitions);

    long timestamp = dateToTimeStamp("09-11-2022 14:00:00");
    Map<TopicPartition, Long> partitionOffsetsRequest = new HashMap<>(partitions.size());
    for (TopicPartition partition : partitions) {
      partitionOffsetsRequest.put(new TopicPartition(partition.topic(), partition.partition()),
        timestamp);
    }

    final Map<TopicPartition, Long> result = new HashMap<>(partitions.size());

    for (Map.Entry<TopicPartition, OffsetAndTimestamp> partitionToOffset : kafkaConsumer.offsetsForTimes(
      partitionOffsetsRequest).entrySet()) {
      result.put(new TopicPartition(partitionToOffset.getKey().topic(),
          partitionToOffset.getKey().partition()),
        (partitionToOffset.getValue() == null) ? null : partitionToOffset.getValue().offset());
    }

    System.out.println(result);

    for (TopicPartition part : result.keySet()) {
      long offset = result.get(part);
      kafkaConsumer.seek(part, offset);
    }

    System.out.println("trying to get records...");
    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
    for (ConsumerRecord<String, String> record : records) {
      Date date = new Date(record.timestamp());
      DateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
      formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
      String dateFormatted = formatter.format(date);
      System.out.println(
        "Received message: (" + record.key() + ", " + record.value() + ") at offset "
          + record.offset() + " at time " + dateFormatted);
    }
  }

  private static long dateToTimeStamp(String date) {
    String pattern = "dd-MM-yyyy HH:mm:ss";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern).withZone(ZoneOffset.UTC);
    LocalDateTime localDateTime = LocalDateTime.from(formatter.parse(date));

    return Timestamp.valueOf(localDateTime).getTime();
  }
}
