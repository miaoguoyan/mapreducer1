package com.baizhi.demo10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ClickReducer extends Reducer<Text, IntWritable,String,String> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total=0;
        for (IntWritable value : values) {
            total+=value.get();
        }
        context.write(key.toString(),total+"");
    }
}
