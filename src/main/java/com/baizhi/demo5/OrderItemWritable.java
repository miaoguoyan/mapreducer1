package com.baizhi.demo5;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderItemWritable implements Writable {
    private String itemName;
    private double price;

    public OrderItemWritable(String itemName, double price) {
        this.itemName = itemName;
        this.price = price;
    }

    public OrderItemWritable() {
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    /**
     * 序列化方法，用于将Map端输出结果持久化到磁盘
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(itemName);
        out.writeDouble(price);
    }

    /**
     * 反序列化方法，用于将磁盘结果反序列化成对象
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        itemName = in.readUTF();
        price = in.readDouble();
    }
}
