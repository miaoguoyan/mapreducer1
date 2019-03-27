package com.baizhi.demo5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OrderReducer extends Reducer<Text,OrderItemWritable,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<OrderItemWritable> values, Context context) throws IOException, InterruptedException {
        List<String> items=new ArrayList<String>();
        double totalPirce =0.0;
        for (OrderItemWritable v : values) {
            items.add(v.getItemName());
            totalPirce += v.getPrice();
        }
        context.write(key,new Text(items+" "+totalPirce));
    }
}
