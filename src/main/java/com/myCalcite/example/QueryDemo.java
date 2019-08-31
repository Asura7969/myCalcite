package com.myCalcite.example;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.*;
import java.util.Properties;

// https://www.jianshu.com/p/4f4fea8abfab
public class QueryDemo {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        /**
         * 注册一个对象作为 schema ，通过反射读取 JavaHrSchema 对象内部结构，将其属性 employee 和 department 作为表
         */
        rootSchema.add("hr", new ReflectiveSchema(new JavaHrSchema()));
        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(
                "select e.emp_id, e.name as emp_name, e.dept_no, d.name as dept_name "
                        + "from hr.employee as e "
                        + "left join hr.department as d on e.dept_no = d.dept_no");
        /**
         * 遍历 SQL 执行结果
         */
        while (resultSet.next()) {
            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                System.out.print(resultSet.getMetaData().getColumnName(i) + ":" + resultSet.getObject(i));
                System.out.print(" | ");
            }
            System.out.println();
        }

        resultSet.close();
        statement.close();
        connection.close();

    }
}
