package com.myCalcite.example.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.LinkedHashMap;
import java.util.Map;

public class JsonRowConverter implements KafkaRowConverter {

    private LinkedHashMap<String,String> schema;
    private static final String MSG_PARTITION = "MSG_PARTITION";
    private static final String MSG_TIMESTAMP = "MSG_TIMESTAMP";
    private static final String MSG_OFFSET = "MSG_OFFSET";

    public JsonRowConverter(LinkedHashMap<String, String> schema) {
        this.schema = schema;
    }

    public LinkedHashMap<String, String> getSchema() {
        return schema;
    }

    public void setSchema(LinkedHashMap<String, String> schema) {
        this.schema = schema;
    }

    @Override
    public RelDataType rowDataType(String topicName) {
        return buildType(schema);
    }

    @Override
    public Object[] toRow(ConsumerRecord message) {
        Object[] fields = new Object[schema.size() + 3];
        fields[0] = message.partition();
        fields[1] = message.timestamp();
        fields[2] = message.offset();

        JSONObject value = JSON.parseObject(message.value().toString());

        int i = 3;
        for (Map.Entry<String, String> entry : schema.entrySet()) {
            fields[i] = value.get(entry.getKey());
            i ++ ;
        }

        return fields;
    }

    private RelDataType buildType(LinkedHashMap<String,String> schema){
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();

        fieldInfo.add(MSG_PARTITION, typeFactory.createSqlType(SqlTypeName.INTEGER)).nullable(false);
        fieldInfo.add(MSG_TIMESTAMP, typeFactory.createSqlType(SqlTypeName.BIGINT)).nullable(false);
        fieldInfo.add(MSG_OFFSET, typeFactory.createSqlType(SqlTypeName.BIGINT)).nullable(false);

        for (Map.Entry<String, String> kv : schema.entrySet()) {
            fieldInfo.add(kv.getKey().toUpperCase(), typeFactory.createSqlType(checkType(kv.getValue()))).nullable(false);
        }

        return fieldInfo.build();
    }

    private SqlTypeName checkType(String type){
        switch (type.toLowerCase()){
            case "int":
                return SqlTypeName.VARCHAR;
            case "long":
                return SqlTypeName.BIGINT;
            case "double":
                return SqlTypeName.DOUBLE;
            case "string":
                return SqlTypeName.VARCHAR;
            case "boolean":
                return SqlTypeName.BOOLEAN;
            default:
                throw new RuntimeException(String.format("column 不支持的类型:%s",type));
        }
    }
}
