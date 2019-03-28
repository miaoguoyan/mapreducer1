package com.baizhi.demo10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RedisOutpoutFormat extends OutputFormat<String,String> {
    public RecordWriter<String, String> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        RedisConfiguration redisConf = new RedisConfiguration(conf);
        return new RedisHashRecordWriter(redisConf.getHost(),redisConf.getPort(),redisConf.getDescriptKey());
    }

    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException { }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
                context);
    }
}
