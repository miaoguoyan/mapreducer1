package com.baizhi.demo10;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RedisOutputForamt extends OutputFormat<String,String> {

    public class KeyValueRecordWriter extends RecordWriter<String,String>{

        public KeyValueRecordWriter(String host,int port){
            //构建Jedisd对象
        }

        public void write(String key, String value) throws IOException, InterruptedException {
            System.out.println(key+" "+value);
            //调用jedisAPI 将数据插入Redis数据库
        }

        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            //关闭jdeis链接
        }
    }
    public RecordWriter<String, String> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        String host = context.getConfiguration().get("redis.host", "localhost");
        int port = context.getConfiguration().getInt("redis.port", 6379);
        return new KeyValueRecordWriter(host,port);
    }

    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException { }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
         return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
                context);
    }
}
