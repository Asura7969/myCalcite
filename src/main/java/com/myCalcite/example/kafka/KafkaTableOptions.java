package com.myCalcite.example.kafka;

import org.apache.kafka.clients.consumer.Consumer;

import java.util.Map;

/**
 * Available options for {@link KafkaStreamTable}.
 */
public final class KafkaTableOptions {
  private String bootstrapServers;
  private String topicName;
  private String groupId;
  private String keyDeserializer;
  private String valueDeserializer;

  private KafkaRowConverter rowConverter;
  private Map<String, String> consumerParams;
  //added to inject MockConsumer for testing.
  private Consumer consumer;

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public String getKeyDeserializer() {
    return keyDeserializer;
  }

  public void setKeyDeserializer(String keyDeserializer) {
    this.keyDeserializer = keyDeserializer;
  }

  public String getValueDeserializer() {
    return valueDeserializer;
  }

  public void setValueDeserializer(String valueDeserializer) {
    this.valueDeserializer = valueDeserializer;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public KafkaTableOptions setBootstrapServers(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
    return this;
  }

  public String getTopicName() {
    return topicName;
  }


  public KafkaTableOptions setTopicName(final String topicName) {
    this.topicName = topicName;
    return this;
  }

  public KafkaRowConverter getRowConverter() {
    return rowConverter;
  }

  public KafkaTableOptions setRowConverter(
      final KafkaRowConverter rowConverter) {
    this.rowConverter = rowConverter;
    return this;
  }

  public Map<String, String> getConsumerParams() {
    return consumerParams;
  }

  public KafkaTableOptions setConsumerParams(final Map<String, String> consumerParams) {
    this.consumerParams = consumerParams;
    return this;
  }

  public Consumer getConsumer() {
    return consumer;
  }

  public KafkaTableOptions setConsumer(final Consumer consumer) {
    this.consumer = consumer;
    return this;
  }
}