package com.baizhi.demo9;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomJobSubmitter extends Configured implements Tool {
    public int run(String[] strings) throws Exception {
        //1.封装Job对象
        Configuration conf = getConf();
        conf.setInt(MRJobConfig.NUM_MAPS,3);
        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.0.125:3306/test",
                "root",
                "1234"
        );
        Job job=Job.getInstance(conf);

        job.setJarByClass(CustomJobSubmitter.class);

        //2.设置任务的读取、写出数据格式
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //3.设置数据读入和写出路径\
        String inputQuery="select id,name,age,items,price from tt_user";
        String inputCountQuery="select count(*) from tt_user";
        DBInputFormat.setInput(job,UserOrderDBWritable.class,inputQuery,inputCountQuery);

        Path dst = new Path("/demo/result");//必须不存在，否则任务提交失败
        TextOutputFormat.setOutputPath(job,dst);
        //4.设置数据处理逻辑代码片段
        job.setMapperClass(UserOrderMapper.class);
        job.setReducerClass(UserOrderReducer.class);
        //5.设置Mapper和Reducer输出key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //6.任务提交
        //job.submit();
        job.waitForCompletion(true);
        return 0;
    }
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CustomJobSubmitter(),args );
    }
}
