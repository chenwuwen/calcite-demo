package com.kanyun.calcite;

import com.kanyun.calcite.column.MemoryColumn;
import com.kanyun.calcite.schema.MemorySchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 查询内存(没有model.json文件)
 */
public class CalciteMemoryTest {

    Properties info;
    Connection connection;
    Statement statement;
    ResultSet resultSet;

    public void getData(List<MemoryColumn> meta, List<List<Object>> source) throws SQLException, UnsupportedEncodingException {
        System.setProperty("saffron.default.charset","UTF-8");
//         构造Schema
        Schema memory = new MemorySchema(meta, source);
//         设置连接参数
        info = new Properties();
        info.setProperty(CalciteConnectionProperty.DEFAULT_NULL_COLLATION.camelName(), NullCollation.LAST.name());
//        SQL大小写敏感
        info.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
//         建立连接
        connection = DriverManager.getConnection("jdbc:calcite:", info);

//         执行查询
        statement = connection.createStatement();
//         取得Calcite连接
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
//         取得RootSchema RootSchema是所有Schema的父Schema
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
//         添加schema
        rootSchema.add("memory", memory);
        // 编写SQL
//        String sql = "select * from memory.memory where COALESCE (id, 2) <> 2 order by id asc";
//        String sql = "select * from memory.memory where id = 2 ";
        String s = new String("丽莎".getBytes(), "UTF-8");
//        String sql = "select * from memory.memory where name =  '" + s + "'";
        String sql = "select * from memory.memory where name = '丽莎'";
//        String sql = "select COALESCE (id, 77) ,name ,age from memory.memory  order by id asc";
//        String sql = "select age from memory.memory group by age ";
//        statement.setEscapeProcessing(true);
        resultSet = statement.executeQuery(sql);

        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + ":" + resultSet.getString(2) + ":" + resultSet.getString(3));
//            System.out.println(resultSet.getString(1) );
        }

        resultSet.close();
        statement.close();
        connection.close();
    }

    public static void main(String[] args) throws SQLException, UnsupportedEncodingException {
        List<MemoryColumn> meta = new ArrayList<>();
        List<List<Object>> source = new ArrayList<>();
        MemoryColumn id = new MemoryColumn("id", Long.class);
        MemoryColumn name = new MemoryColumn("name", String.class);
        MemoryColumn age = new MemoryColumn("age", Integer.class);
        meta.add(id);
        meta.add(name);
        meta.add(age);

        List<Object> line1 = new ArrayList<Object>() {
            {
                add(null);
                add("张珊");
                add(25);
            }
        };
        List<Object> line2 = new ArrayList<Object>() {
            {
                add(2L);
                add("丽莎");
                add(25);
            }
        };
        List<Object> line3 = new ArrayList<Object>() {
            {
                add(3L);
                add("无为");
                add(52);
            }
        };
        List<Object> line4 = new ArrayList<Object>() {
            {
                add(null);
                add("奇才");
                add(48);
            }
        };
        source.add(line1);
        source.add(line2);
        source.add(line4);
        source.add(line3);
        new CalciteMemoryTest().getData(meta, source);
    }

}
