package com.baizhi.demo7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UserOrderReducer extends Reducer<Text, Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> items=new ArrayList<String>();
        String userInfo=null;
        double totalPirce =0.0;
        for (Text v : values) {
            String s = v.toString();
            if(s.endsWith("_USER")){
                userInfo=s.replace("_USER","");
            }else if(s.endsWith("_ORDER")){
                s=s.replace("_ORDER","");
                String[] splits = s.split(":");
                items.add(splits[0]);
                totalPirce += Double.parseDouble(splits[1]);
            }
        }
        context.write(key,new Text(userInfo+" " +items+" "+totalPirce));
    }
}
