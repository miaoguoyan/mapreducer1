package com.baizhi.demo7;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserMappper extends Mapper<LongWritable, Text,Text, Text> {
    //003 wangwu true 30
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        String userid=tokens[0];
        String username=tokens[1];
        String age=tokens[3];

        context.write(new Text(userid),new Text(username+" "+age+"_USER"));
    }
}
