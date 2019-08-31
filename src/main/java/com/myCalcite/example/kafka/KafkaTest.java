package com.myCalcite.example.kafka;

import org.apache.calcite.util.Sources;

import java.io.File;
import java.sql.*;

import java.util.Properties;

/**
 * kafka msg:
 * "{\"metric\": \"request-server\",\"iface\":\"com.gwz.gateway\",\"rpcid\":\"1.1\",\"logValue\":50.0}"
 */
public class KafkaTest {

    public static void main(String[] args) throws SQLException {

        String sql = "SELECT STREAM * from JSONTABLE WHERE LOGVALUE = 50.0";

        //Properties info = new Properties();
        //info.put("model", Sources.of(new File("/Users/gongwenzhou/IdeaProjects/myCalcite/src/main/java/com/myCalcite/example/kafka/kafka-model.json")));

        String model = "{\n" +
                "  \"version\": \"1.0\",\n" +
                "  \"defaultSchema\": \"KAFKA\",\n" +
                "  \"schemas\": [\n" +
                "    {\n" +
                "      \"name\": \"KAFKA\",\n" +
                "      \"tables\":[\n" +
                "        {\n" +
                "          \"name\":\"JSONTABLE\",\n" +
                "          \"type\": \"custom\",\n" +
                "          \"factory\": \"com.myCalcite.example.kafka.KafkaTableFactory\",\n" +
                "          \"operand\": {\n" +
                "            \"converter.type\": \"json\",\n" +
                "            \"data.type\": {\n" +
                "              \"metric\":\"string\",\n" +
                "              \"iface\":\"string\",\n" +
                "              \"rpcid\":\"string\",\n" +
                "              \"logValue\":\"double\"\n" +
                "            },\n" +
                "            \"consumer.params\": {\n" +
                "              \"bootstrap.servers\": \"47.99.61.133:9092,47.110.148.212:9092,47.110.225.177:9092\",\n" +
                "              \"topic.name\": \"wy-test\",\n" +
                "              \"group.id\": \"test\",\n" +
                "              \"key.deserializer\": \"org.apache.kafka.common.serialization.StringDeserializer\",\n" +
                "              \"value.deserializer\": \"org.apache.kafka.common.serialization.StringDeserializer\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        Connection connection = DriverManager.getConnection("jdbc:calcite:model=inline:"+model);
        Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(sql);

        while (resultSet.next()) {
            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                System.out.print(resultSet.getMetaData().getColumnName(i) + ":" + resultSet.getObject(i).toString());
                System.out.print(" | ");
            }
            System.out.println();
        }

        statement.close();
        connection.close();


    }
}
