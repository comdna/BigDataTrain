package com.lyf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OutputFormatDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.设置jar包路径
        job.setJarByClass(OutputFormatDriver.class);
        //3.关联mapper和reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        //4.设置mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //5.设置最终的输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置其他
        job.setOutputFormatClass(MyoutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/home/hadoop/eclipse-workspace/outputFormatDemo/input"));
        FileOutputFormat.setOutputPath(job, new Path("/home/hadoop/eclipse-workspace/outputFormatDemo/outputR"));
        //7.提交job
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
