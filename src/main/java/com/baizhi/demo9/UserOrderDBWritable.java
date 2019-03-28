package com.baizhi.demo9;


import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class UserOrderDBWritable implements DBWritable {
    private String id;
    private String name;
    private int age;
    private String items="";
    private double price;

    public UserOrderDBWritable() {
    }

    public UserOrderDBWritable(String id, String name, int age, List<String> items, double price) {
        this.id = id;
        this.name = name;
        this.age = age;
        for (String item : items) {
            this.items += item+",";
        }
        this.price = price;
    }
   //"id","name","age","items","price
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1,id);
        statement.setString(2,name);
        statement.setInt(3,age);
        statement.setString(4,items);
        statement.setDouble(5,price);
    }

    public void readFields(ResultSet resultSet) throws SQLException {
         id=resultSet.getString("id");
         name=resultSet.getString("name");
         age=resultSet.getInt("age");
         items=resultSet.getString("items");
         price=resultSet.getDouble("price");

    }

    @Override
    public String toString() {
        return id + ' '  + name + ' '  + age + " " + items + ' ' +" " + price ;
    }
}
