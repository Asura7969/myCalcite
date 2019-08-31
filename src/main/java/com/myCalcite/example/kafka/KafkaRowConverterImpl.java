package com.myCalcite.example.kafka;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaRowConverterImpl implements KafkaRowConverter<String, String>{


    @Override
    public RelDataType rowDataType(String topicName) {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();

        fieldInfo.add("MSG_PARTITION", typeFactory.createSqlType(SqlTypeName.INTEGER)).nullable(false);
        fieldInfo.add("MSG_TIMESTAMP", typeFactory.createSqlType(SqlTypeName.BIGINT)).nullable(false);
        fieldInfo.add("MSG_OFFSET", typeFactory.createSqlType(SqlTypeName.BIGINT)).nullable(false);
        //fieldInfo.add("MSG_KEY_BYTES", typeFactory.createSqlType(SqlTypeName.VARCHAR)).nullable(true);
        fieldInfo.add("MSG_VALUE_BYTES", typeFactory.createSqlType(SqlTypeName.VARCHAR)).nullable(false);

        return fieldInfo.build();
    }

    @Override
    public Object[] toRow(ConsumerRecord<String, String> message) {
        Object[] fields = new Object[5];
        fields[0] = message.partition();
        fields[1] = message.timestamp();
        fields[2] = message.offset();
        //fields[3] = message.key();
        fields[3] = message.value();

        return fields;
    }
}
