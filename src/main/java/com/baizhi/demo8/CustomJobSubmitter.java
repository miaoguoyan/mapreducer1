package com.baizhi.demo8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomJobSubmitter extends Configured implements Tool {
    public int run(String[] strings) throws Exception {
        //1.封装Job对象
        Configuration conf = getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator","|");
        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/test",
                "root",
                "1234"
                );
        Job job=Job.getInstance(conf);

        //2.设置任务的读取、写出数据格式
        job.setOutputFormatClass(DBOutputFormat.class);
        //3.设置数据读入和写出路径
        DBOutputFormat.setOutput(job,"user","id","name","age","items","price");

        //4.设置数据处理逻辑代码片段
        job.setReducerClass(UserOrderReducer.class);

        MultipleInputs.addInputPath(job,new Path("file:///D:/demo/user"), TextInputFormat.class, UserMappper.class);
        MultipleInputs.addInputPath(job,new Path("file:///D:/demo/order"), KeyValueTextInputFormat.class, OrderMappper.class);

        //5.设置Mapper和Reducer输出key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(UserOrderDBWritable.class);
        job.setOutputValueClass(Text.class);

        //6.任务提交
        //job.submit();
        job.waitForCompletion(true);
        return 0;
    }
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CustomJobSubmitter(),args );
    }
}
