package com.baizhi.demo8;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UserOrderReducer extends Reducer<Text, Text,UserOrderDBWritable,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> items=new ArrayList<String>();
        String username=null;
        int age=0;
        double totalPirce =0.0;
        for (Text v : values) {
            String s = v.toString();
            if(s.endsWith("_USER")){
                s=s.replace("_USER","");
                username=s.split(" ")[0];
                age = Integer.parseInt(s.split(" ")[1]);
            }else if(s.endsWith("_ORDER")){
                s=s.replace("_ORDER","");
                String[] splits = s.split(":");
                items.add(splits[0]);
                totalPirce += Double.parseDouble(splits[1]);
            }
        }
        UserOrderDBWritable uow = new UserOrderDBWritable(key.toString(), username, age, items, totalPirce);
        System.out.println(uow);
        context.write(uow,null);
    }
}
