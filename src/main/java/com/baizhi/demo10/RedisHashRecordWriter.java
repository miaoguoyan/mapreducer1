package com.baizhi.demo10;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;

public class RedisHashRecordWriter extends RecordWriter<String,String> {
    private Jedis jedis;
    private String descriptKey;
    private Pipeline pipeline;

    public RedisHashRecordWriter(String host,int port,String descriptKey) {
        this.jedis = new Jedis(host,port);
        pipeline=jedis.pipelined();
        this.descriptKey=descriptKey;
    }

    public void write(String key, String value) throws IOException, InterruptedException {
        //启用Redis的批处理
        pipeline.hset(descriptKey,key,value);
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        pipeline.sync();//批量提交
        jedis.close();//关闭链接
    }
}
