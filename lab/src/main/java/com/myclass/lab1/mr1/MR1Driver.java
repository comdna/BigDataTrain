/*MR1Driver */
package com.myclass.lab1.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MR1Driver {
    public MR1Driver(){}
    public static void main(String[] args) throws Exception {
        //获取Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar包
        job.setJarByClass(MR1Driver.class);
        //关联mapper reducer
        job.setMapperClass(MR1Mapper.class);
        job.setReducerClass(MR1Reducer.class);
        //设置mapper输出kv
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MoneyBean.class);
        //设置最终输出kv
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MoneyBean.class);
        //设置输入输出路径
        //本地操作
		FileInputFormat.addInputPath(job, new Path("/home/hadoop/eclipse-workspace/lab/input"));
		FileOutputFormat.setOutputPath(job, new Path("/home/hadoop/eclipse-workspace/lab/output/mr1"));
		////远程操作
		//String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
		// if(otherArgs.length < 2) {
		// 	System.err.println("Usage: wordcount <in> [<in>...] <out>");
		// 	System.exit(2);
		// }
		// for(int i = 0; i < otherArgs.length - 1; ++i) {
		// 	FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		// }
		// FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		////7.提交job
		System.exit(job.waitForCompletion(true)?0:1);
    }
}

