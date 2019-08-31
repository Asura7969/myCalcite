package com.myCalcite.example.kafka;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class KafkaTableFactory implements TableFactory<KafkaStreamTable> {
  public KafkaTableFactory() {
  }

  @Override public KafkaStreamTable create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {

    final KafkaTableOptions tableOptionBuilder = new KafkaTableOptions();
    LinkedHashMap<String,Object> param = (LinkedHashMap<String,Object>)operand.get(KafkaTableConstants.SCHEMA_CONSUMER_PARAMS);
    tableOptionBuilder.setBootstrapServers(
        (String) param.getOrDefault(KafkaTableConstants.SCHEMA_BOOTSTRAP_SERVERS, null));
    tableOptionBuilder.setTopicName(
        (String) param.getOrDefault(KafkaTableConstants.SCHEMA_TOPIC_NAME, null));

    KafkaRowConverter rowConverter = new KafkaRowConverterImpl();
    if (operand.containsKey(KafkaTableConstants.SCHEMA_CONVERTER_TYPE)) {
      String type = (String) operand.get(KafkaTableConstants.SCHEMA_CONVERTER_TYPE);

      LinkedHashMap<String,String> dataType = (LinkedHashMap<String,String>)operand.get(KafkaTableConstants.SCHEMA_DATA_TYPE);
      switch (type.toUpperCase()){
        case "JSON":
          rowConverter = new JsonRowConverter(dataType);
          break;

        default:
          throw new RuntimeException(String.format("不支持的转化格式:%s",type));
      }
    }

    tableOptionBuilder.setRowConverter(rowConverter);

    if (param.containsKey(KafkaTableConstants.SCHEMA_KEY_DESERIALIZER) &&
            param.containsKey(KafkaTableConstants.SCHEMA_VALUE_DESERIALIZER)) {
      tableOptionBuilder.setKeyDeserializer((String) param.get(KafkaTableConstants.SCHEMA_KEY_DESERIALIZER));
      tableOptionBuilder.setValueDeserializer((String) param.get(KafkaTableConstants.SCHEMA_VALUE_DESERIALIZER));
    } else {
      throw new RuntimeException("No params:key.deserializer,value.deserializer...");
    }
    //if (operand.containsKey(KafkaTableConstants.SCHEMA_CUST_CONSUMER)) {
    //  String custConsumerClass = (String) operand.get(KafkaTableConstants.SCHEMA_CUST_CONSUMER);
    //  try {
    //    tableOptionBuilder.setConsumer(
    //        (Consumer) Class.forName(custConsumerClass)
    //            .getConstructor(OffsetResetStrategy.class)
    //            .newInstance(OffsetResetStrategy.NONE));
    //  } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException
    //      | InstantiationException | InvocationTargetException e) {
    //    final String details = String.format(
    //        Locale.ROOT,
    //        "Fail to create table '%s' with configuration: \n"
    //            + "'%s'\n"
    //            + "KafkaCustConsumer '%s' is invalid",
    //        name, operand, custConsumerClass);
    //    throw new RuntimeException(details, e);
    //  }
    //}

    tableOptionBuilder.setGroupId((String)param.get("group.id"));

    tableOptionBuilder.setConsumer(getConsumer(tableOptionBuilder));
    return new KafkaStreamTable(tableOptionBuilder);
  }

  private KafkaConsumer getConsumer(KafkaTableOptions tableOptionBuilder){
    Properties kafkaConf = new Properties();

    kafkaConf.put("bootstrap.servers", tableOptionBuilder.getBootstrapServers());
    kafkaConf.put("group.id", tableOptionBuilder.getGroupId());
    kafkaConf.put("key.deserializer", tableOptionBuilder.getValueDeserializer());
    kafkaConf.put("value.deserializer", tableOptionBuilder.getKeyDeserializer());

    KafkaConsumer<String,String> consumer = new KafkaConsumer<>(kafkaConf);
    consumer.subscribe(Arrays.asList(tableOptionBuilder.getTopicName().split(",")));
    return consumer;
  }
}