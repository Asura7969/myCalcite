package com.myCalcite.example.kafka;

interface KafkaTableConstants {
  String SCHEMA_TOPIC_NAME = "topic.name";
  String SCHEMA_BOOTSTRAP_SERVERS = "bootstrap.servers";
  String SCHEMA_GROUP_ID = "group.id";


  String SCHEMA_KEY_DESERIALIZER = "key.deserializer";
  String SCHEMA_VALUE_DESERIALIZER = "value.deserializer";

  String SCHEMA_CONSUMER_PARAMS = "consumer.params";
  //String SCHEMA_ROW_CONVERTER = "row.converter";
  String SCHEMA_CONVERTER_TYPE = "converter.type";
  String SCHEMA_DATA_TYPE = "data.type";
}