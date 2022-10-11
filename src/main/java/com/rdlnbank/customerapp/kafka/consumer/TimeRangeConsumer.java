package com.rdlnbank.customerapp.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

@Slf4j
public final class TimeRangeConsumer {
  public static List<String> consumeMessages(String from, String until) throws ParseException {
    Properties kafkaProperties = new Properties();
    kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "transaction-group");
    kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.TRUE.toString());
    // kafkaProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    kafkaProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    kafkaProperties.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaProperties.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    kafkaProperties.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProperties.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties);
    String topicName = "transaction-request";
    Producer producer = new KafkaProducer(kafkaProperties);
    int partitionsNumber = producer.partitionsFor(topicName).size();
    long start = dateToTimeStamp(from); // dateToTimeStamp("09-11-2022 15:49:00");
    long end = dateToTimeStamp(until); // dateToTimeStamp("09-11-2022 15:52:00");
    log.info("Start timeStamp : " + start);
    log.info("End timeStamp : " + end);
    TopicPartition[] topicPartitions = new TopicPartition[partitionsNumber];
    Map<TopicPartition, Long> timeStamps = new HashMap<>();

    for (int i = 0; i < partitionsNumber; i++) {
      TopicPartition partition = new TopicPartition(topicName, i);
      topicPartitions[i] = partition;
      timeStamps.put(partition, start);
    }

    consumer.assign(Arrays.asList(topicPartitions));

    Map<TopicPartition, OffsetAndTimestamp> partitionOffsetMap = consumer.offsetsForTimes(timeStamps);

    for (int i = 0; i < partitionsNumber; i++) {
      OffsetAndTimestamp offsetAndTimestamp = partitionOffsetMap.get(topicPartitions[i]);
      if (offsetAndTimestamp != null) {
        long offset = offsetAndTimestamp.offset();
        consumer.seek(topicPartitions[i], offset);
      }
    }

    boolean flag = true;
    long count = 0L;
    List<String> resultList = new ArrayList<>();
    while (flag) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
      count = count + records.count();
      log.info("Record fetch count : " + count);
      if (records != null && !records.isEmpty()) {
        for (ConsumerRecord<String, String> record : records) {
          if (record.timestamp() >= end) {
            flag = false;
            break;
          } else {
            log.info("TimeStamp : " + record.timestamp());
            resultList.add(record.value());
          }
        }
      } else {
        break;
      }
    }

    consumer.close();
    return resultList;
  }

  private static long dateToTimeStamp(String stringDate) throws ParseException {
    Date date = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss").parse(stringDate);
    return date.getTime();
  }
}
