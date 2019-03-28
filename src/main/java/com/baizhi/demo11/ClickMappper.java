package com.baizhi.demo11;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ClickMappper  extends Mapper<LongWritable, Text,Text, Text> {
    //INFO /product 001 2019-03-26 10:00:00
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        context.write(new Text(tokens[1]),new Text(tokens[2]));
    }
}
