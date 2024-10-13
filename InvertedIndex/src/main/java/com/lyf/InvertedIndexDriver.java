package com.lyf;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexDriver {
	public InvertedIndexDriver() {}
	public static void main(String[] args) throws Exception {
		//1.获取job实例
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//2.设置jar包路径
		job.setJarByClass(InvertedIndexDriver.class);
		//3.关联mapper和reducer
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		//4.设置mapper输出的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//5.设置最终的输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//6.输入输出路径
		
		//本地操作
		FileInputFormat.addInputPath(job, new Path("/home/hadoop/eclipse-workspace/InvertedIndex/src/main/input"));
		FileOutputFormat.setOutputPath(job, new Path("/home/hadoop/eclipse-workspace/InvertedIndex/src/main/output"));
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
		//7.提交job
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
