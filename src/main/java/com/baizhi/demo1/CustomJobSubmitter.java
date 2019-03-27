package com.baizhi.demo1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomJobSubmitter extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        //1.封装Job对象
        Job job= Job.getInstance(getConf());

        job.setJarByClass(CustomJobSubmitter.class);

        //2.设置任务的读取、写出数据格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //3.设置数据读入和写出路径
        Path src = new Path("/demo/click");
        TextInputFormat.addInputPath(job,src);
        Path dst = new Path("/demo/result");//必须不存在，否则任务提交失败
        TextOutputFormat.setOutputPath(job,dst);
        //4.设置数据处理逻辑代码片段
        job.setMapperClass(ClickMappper.class);
        job.setReducerClass(ClickReducer.class);
        //5.设置Mapper和Reducer输出key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.任务提交
        //job.submit();
        job.waitForCompletion(true);
        return 0;
    }
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CustomJobSubmitter(),args );
    }
}
