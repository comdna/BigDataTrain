package main.java;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	public WordCount() {}
	public static void main(String[] args) throws Exception {
		// Configuration conf = new Configuration();
		// String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
		// if(otherArgs.length < 2) {
		// 	System.err.println("Usage: wordcount <in> [<in>...] <out>");
		// 	System.exit(2);
		// }
		// //job设置mapper和reducer的class
		// Job job = Job.getInstance(conf, "word count");
		// job.setJarByClass(WordCount.class);
		// job.setMapperClass(CountMapper.class);
		// job.setCombinerClass(CountReducer.class);
		// job.setReducerClass(CountReducer.class);
        // //设置最终输出类型
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(IntWritable.class); 
		// //读取输入文件并设置输出文件的位置
		// for(int i = 0; i < otherArgs.length - 1; ++i) {
		// 	FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		// }
		// FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		// System.exit(job.waitForCompletion(true)?0:1);

		//1.获取job实例
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//2.设置jar包路径
		job.setJarByClass(WordCount.class);
		//3.关联mapper和reducer
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		//4.设置mapper输出的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//5.设置最终的输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//6.输入输出路径
		String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
		if(otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		for(int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		//7.提交job
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
