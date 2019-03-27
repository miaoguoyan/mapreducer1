package com.baizhi.demo7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class OrderMappper extends Mapper<Text, Text,Text, Text> {
    //001	1,苹果,4.5,2,2019-03-27 12:00:00
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String itemName=tokens[1];
        double price=Double.parseDouble(tokens[2])*Integer.parseInt(tokens[3]);

        context.write(key,new Text(itemName+":"+price+"_ORDER"));
    }
}
