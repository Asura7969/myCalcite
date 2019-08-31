package com.myCalcite.example.kafka;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KafkaRowConverter<K, V> {

  /**
   * Generates row type for a given Kafka topic.
   *
   * @param topicName, Kafka topic name;
   * @return row type
   */
  RelDataType rowDataType(String topicName);

  /**
   * Parses and reformats Kafka message from consumer,
   * to align with row type defined as {@link #rowDataType(String)}.
   *
   * @param message, the raw Kafka message record;
   * @return fields in the row
   */
  Object[] toRow(ConsumerRecord<K, V> message);
}