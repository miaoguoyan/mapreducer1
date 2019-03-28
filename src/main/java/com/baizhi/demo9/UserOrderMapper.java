package com.baizhi.demo9;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserOrderMapper extends Mapper<LongWritable, UserOrderDBWritable,Text, NullWritable> {
    /*      +------+----------+------+----------------+-------+
            | id   | name     | age  | items          | price |
            +------+----------+------+----------------+-------+
            | 001  | zhangsan |   18 | 香蕉,苹果, |    16 |
            | 002  | lisi     |   25 | Iphone X,      |  1500 |
            | 003  | wangwu   |   30 | 机械键盘,  |  1200 |
            | 004  | zhaoliu  |   20 |                |     0 |
            +------+----------+------+----------------+-------+
     */
    @Override
    protected void map(LongWritable key, UserOrderDBWritable value, Context context) throws IOException, InterruptedException {
            context.write(new Text(value.toString()),NullWritable.get());
    }
}
